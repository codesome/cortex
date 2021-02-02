package distributor

import (
	"context"
	"flag"
	"github.com/go-kit/kit/log/level"
	"hash/fnv"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertmanagerpb"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
)

// RingOp is the operation used for distributing tenants between alertmanagers.
var RingOp = ring.NewOp([]ring.IngesterState{ring.ACTIVE}, func(s ring.IngesterState) bool {
	// Only ACTIVE Alertmanager get requests. If instance is not ACTIVE, we need to find another Alertmanager.
	return s != ring.ACTIVE
})

// Config contains the configuration required to
// create a Distributor
type Config struct {
	RemoteTimeout      time.Duration            `yaml:"remote_timeout"`
	AlertmanagerClient AlertmanagerClientConfig `yaml:"alertmanager_client"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.AlertmanagerClient.RegisterFlagsWithPrefix("alertmanager-distributor.alertmanager-client", f)
	f.DurationVar(&cfg.RemoteTimeout, "alertmanager.distributor.remote-timeout", 2*time.Second, "Timeout for downstream alertmanagers.")
}

// Distributor forwards requests to individual alertmanagers.
type Distributor struct {
	services.Service

	cfg              Config
	requestsInFlight sync.WaitGroup

	alertmanagerRing        ring.ReadRing
	alertmanagerClientsPool AlertmanagerClientsPool
	replication             int

	// Manager for subservices (Alertmanager Ring)
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	logger log.Logger

	receivedRequests  *prometheus.CounterVec
	amSends           *prometheus.CounterVec
	amSendFailures    *prometheus.CounterVec
	replicationFactor prometheus.Gauge
}

// New constructs a new Distributor
func New(cfg Config, alertmanagersRing *ring.Ring, alertmanagerClientsPool AlertmanagerClientsPool, logger log.Logger, reg prometheus.Registerer) (d *Distributor, err error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	if alertmanagerClientsPool == nil {
		alertmanagerClientsPool = newAlertmanagerClientsPool(client.NewRingServiceDiscovery(alertmanagersRing), cfg.AlertmanagerClient, logger, reg)
	}

	d = &Distributor{
		cfg:                     cfg,
		logger:                  logger,
		alertmanagerRing:        alertmanagersRing,
		alertmanagerClientsPool: alertmanagerClientsPool,
		replication:             alertmanagersRing.ReplicationFactor(),
	}

	d.initMetrics(reg)
	d.replicationFactor.Set(float64(alertmanagersRing.ReplicationFactor()))

	d.subservices, err = services.NewManager(alertmanagersRing)
	if err != nil {
		return nil, err
	}
	d.subservicesWatcher = services.NewFailureWatcher()
	d.subservicesWatcher.WatchManager(d.subservices)

	d.Service = services.NewBasicService(d.starting, d.running, d.stopping)
	return d, nil
}

func (d *Distributor) initMetrics(r prometheus.Registerer) {
	d.receivedRequests = promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_alertmanager_distributor_received_requests_total",
		Help: "The total number of requests received.",
	}, []string{"user"})
	d.amSends = promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_alertmanager_distributor_alertmanager_send_total",
		Help: "The total number of requests sent to the alertmanager.",
	}, []string{"ingester"})
	d.amSendFailures = promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_alertmanager_distributor_alertmanager_send_failures_total",
		Help: "The total number of requests sent to the alertmanager that failed.",
	}, []string{"ingester"})
	d.replicationFactor = promauto.With(r).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_alertmanager_distributor_replication_factor",
		Help: "The configured replication factor.",
	})
}

func (d *Distributor) starting(ctx context.Context) error {
	return services.StartManagerAndAwaitHealthy(ctx, d.subservices)
}

func (d *Distributor) running(ctx context.Context) error {
	<-ctx.Done()
	d.requestsInFlight.Wait()
	return nil
}

func (d *Distributor) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), d.subservices)
}

// IsPathSupported returns true if the given route is currently supported by the Distributor.
// TODO: This will go away in future after we gradually add support for the entire API.
func (d *Distributor) IsPathSupported(path string) bool {
	// API can be found at https://petstore.swagger.io/?url=https://raw.githubusercontent.com/prometheus/alertmanager/master/api/v2/openapi.yaml.
	return strings.HasSuffix(path, "/alerts") ||
		strings.HasSuffix(path, "/alerts/groups")
}

// DistributeRequest shards the writes and returns as soon as the quorum is satisfied.
// In case of reads, it proxies the request to one of the alertmanagers.
// DistributeRequest assumes that the caller has verified IsPathSupported return
// true for the route.
func (d *Distributor) DistributeRequest(w http.ResponseWriter, r *http.Request) {
	d.requestsInFlight.Add(1)
	defer d.requestsInFlight.Done()

	userID, err := tenant.TenantID(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	d.receivedRequests.WithLabelValues(userID).Inc()

	if r.Method == http.MethodGet {
		d.doRead(userID, w, r)
	} else {
		d.doWrite(userID, w, r)
	}
}

func (d *Distributor) doWrite(userID string, w http.ResponseWriter, r *http.Request) {
	source := util.GetSourceIPsFromOutgoingCtx(r.Context())

	var body []byte
	var err error
	if r.Body != nil {
		body, err = ioutil.ReadAll(r.Body)
		if err != nil {
			level.Error(d.logger).Log("msg", "failed to read the request body during write", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	err = ring.DoBatch(r.Context(), RingOp, d.alertmanagerRing, []uint32{shardByUser(userID)}, func(am ring.IngesterDesc, _ []int) error {
		d.amSends.WithLabelValues(am.Addr).Inc()

		// Use a background context to make sure all alertmanagers get the request even if we return early.
		localCtx, cancel := context.WithTimeout(context.Background(), d.cfg.RemoteTimeout)
		defer cancel()
		localCtx = user.InjectOrgID(localCtx, userID)
		if sp := opentracing.SpanFromContext(r.Context()); sp != nil {
			localCtx = opentracing.ContextWithSpan(localCtx, sp)
		}
		// Get clientIP(s) from Context and add it to localCtx
		localCtx = util.AddSourceIPsToOutgoingContext(localCtx, source)

		resp, err := d.doRequest(localCtx, am, r, body)
		if err != nil {
			d.amSendFailures.WithLabelValues(am.Addr).Inc()
			return err
		}

		if resp.GetStatus() != alertmanagerpb.OK {
			return errors.New("alertmanager grpc request not ok")
		}

		return nil
	}, func() {})

	if err == nil {
		w.WriteHeader(http.StatusOK)
		return
	}

	d.respondFromError(err, w)
}

func (d *Distributor) doRead(userID string, w http.ResponseWriter, r *http.Request) {
	key := shardByUser(userID)
	replicationSet, err := d.alertmanagerRing.Get(key, RingOp, nil, nil, nil)
	if err != nil {
		level.Error(d.logger).Log("msg", "failed to get replication set from the ring", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Until we have a mechanism to combine the results from multiple alertmanagers,
	// we forward the request to only only of the alertmanagers.
	amDesc := replicationSet.Ingesters[0]

	var body []byte
	if r.Body != nil {
		body, err = ioutil.ReadAll(r.Body)
		if err != nil {
			level.Error(d.logger).Log("msg", "failed to read the request body during reads", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	d.amSends.WithLabelValues(amDesc.Addr).Inc()
	resp, err := d.doRequest(r.Context(), amDesc, r, body)
	if err != nil {
		d.amSendFailures.WithLabelValues(amDesc.Addr).Inc()
		d.respondFromError(err, w)
		return
	}

	if resp.GetStatus() != alertmanagerpb.OK {
		http.Error(w, resp.Error, http.StatusInternalServerError)
		return
	}

	http.Error(w, string(resp.HttpResponse.Body), int(resp.HttpResponse.Code))
}

func (d *Distributor) respondFromError(err error, w http.ResponseWriter) {
	httpResp, ok := httpgrpc.HTTPResponseFromError(errors.Cause(err))
	if !ok {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, h := range httpResp.Headers {
		for _, v := range h.Values {
			w.Header().Add(h.Key, v)
		}
	}
	http.Error(w, string(httpResp.Body), int(httpResp.Code))
}

func (d *Distributor) doRequest(ctx context.Context, am ring.IngesterDesc, r *http.Request, requestBody []byte) (*alertmanagerpb.Response, error) {
	amClient, err := d.alertmanagerClientsPool.GetClientFor(am.Addr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get alertmanager from pool %s", am.Addr)
	}

	resp, err := amClient.HandleRequest(ctx, &alertmanagerpb.Request{
		HttpRequest: &httpgrpc.HTTPRequest{
			Method:  r.Method,
			Url:     r.RequestURI,
			Body:    requestBody,
			Headers: httpTogrpchttpHeaders(r.Header),
		},
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to proxy request to alertmanager %s", amClient.RemoteAddress())
	}

	return resp, err
}

func shardByUser(userID string) uint32 {
	ringHasher := fnv.New32a()
	// Hasher never returns err.
	_, _ = ringHasher.Write([]byte(userID))
	return ringHasher.Sum32()
}

func httpTogrpchttpHeaders(hs http.Header) []*httpgrpc.Header {
	result := make([]*httpgrpc.Header, 0, len(hs))
	for k, vs := range hs {
		result = append(result, &httpgrpc.Header{
			Key:    k,
			Values: vs,
		})
	}
	return result
}
