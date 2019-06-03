package index

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ingester/client"
)

func TestIndex(t *testing.T) {
	index := New()

	for _, entry := range []struct {
		m  model.Metric
		fp model.Fingerprint
	}{
		{model.Metric{"foo": "bar", "flip": "flop"}, 3},
		{model.Metric{"foo": "bar", "flip": "flap"}, 2},
		{model.Metric{"foo": "baz", "flip": "flop"}, 1},
		{model.Metric{"foo": "baz", "flip": "flap"}, 0},
	} {
		index.Add(client.FromMetricsToLabelAdapters(entry.m), entry.fp)
	}

	for _, tc := range []struct {
		matchers []*labels.Matcher
		fps      []model.Fingerprint
	}{
		{nil, nil},
		{mustParseMatcher(`{fizz="buzz"}`), []model.Fingerprint{}},

		{mustParseMatcher(`{foo="bar"}`), []model.Fingerprint{2, 3}},
		{mustParseMatcher(`{foo="baz"}`), []model.Fingerprint{0, 1}},
		{mustParseMatcher(`{flip="flop"}`), []model.Fingerprint{1, 3}},
		{mustParseMatcher(`{flip="flap"}`), []model.Fingerprint{0, 2}},

		{mustParseMatcher(`{foo="bar", flip="flop"}`), []model.Fingerprint{3}},
		{mustParseMatcher(`{foo="bar", flip="flap"}`), []model.Fingerprint{2}},
		{mustParseMatcher(`{foo="baz", flip="flop"}`), []model.Fingerprint{1}},
		{mustParseMatcher(`{foo="baz", flip="flap"}`), []model.Fingerprint{0}},
	} {
		assert.Equal(t, tc.fps, index.Lookup(tc.matchers))
	}

	assert.Equal(t, []string{"flip", "foo"}, index.LabelNames())
	assert.Equal(t, []string{"bar", "baz"}, index.LabelValues("foo"))
	assert.Equal(t, []string{"flap", "flop"}, index.LabelValues("flip"))
}

func mustParseMatcher(s string) []*labels.Matcher {
	ms, err := promql.ParseMetricSelector(s)
	if err != nil {
		panic(err)
	}
	return ms
}

func BenchmarkIndexLookup(t *testing.B) {

	cases := []struct {
		numSeries, numSamples, percentageOverlap int
	}{
		{
			numSeries:         10,
			numSamples:        100000,
			percentageOverlap: 50,
		},
	}

	commonLabels := []client.LabelAdapter{
		{"l1", "v1"},
		{"l2", "v2"},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("numSeries=%d,numSamples=%d,percentageOverlap=%d", c.numSeries, c.numSamples, c.percentageOverlap), func(b *testing.B) {

			ii := New()

			// Add series.
			lastEnd := 0
			for i := 0; i < c.numSeries; i++ {
				lbls := make([]client.LabelAdapter, len(commonLabels))
				copy(lbls, commonLabels)
				for j := 0; j < 3; j++ {
					lbls = append(lbls, client.LabelAdapter{randString(), randString()})
				}
				if lastEnd == 0 {
					for j := 0; j < c.numSamples; j++ {
						ii.Add(lbls, model.Fingerprint(j))
					}
					lastEnd = c.numSamples - 1
					continue
				}
				start := lastEnd - (c.percentageOverlap * c.numSamples / 100)
				lastEnd += c.numSamples - 1
				for j := start; j <= lastEnd; j++ {
					ii.Add(lbls, model.Fingerprint(j))
				}
			}

			matchers := make([]*labels.Matcher, 0, len(commonLabels))
			for _, lbl := range commonLabels {
				m, err := labels.NewMatcher(labels.MatchEqual, lbl.Name, lbl.Value)
				require.NoError(b, err)
				matchers = append(matchers, m)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for j := 0; j < b.N; j++ {
				ii.Lookup(matchers)
			}

		})
	}

}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

// randString generates random string.
func randString() string {
	maxLength := int32(50)
	length := rand.Int31n(maxLength)
	b := make([]byte, length+1)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := length, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}
