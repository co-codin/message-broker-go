package main

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics exposed on /metrics when -metrics-addr is set. Intentionally
// unlabeled to keep cardinality bounded — fine for a learning project, and
// easy to add labels later.
var (
	publishesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "minibroker_publishes_total",
		Help: "Total number of messages appended.",
	})
	commitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "minibroker_commits_total",
		Help: "Total number of group offset commits.",
	})
	subscribersActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "minibroker_subscribers_active",
		Help: "Currently-connected subscriptions (ephemeral + group members).",
	})
	segmentRolls = promauto.NewCounter(prometheus.CounterOpts{
		Name: "minibroker_segment_rolls_total",
		Help: "Number of times a partition rolled its active segment.",
	})
	compactionDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "minibroker_compaction_records_dropped_total",
		Help: "Records removed by compaction (superseded by a newer record with the same key).",
	})
)

// startMetricsServer exposes /metrics in Prometheus text format on addr.
// Returns the *http.Server so main can Shutdown it on signal.
func startMetricsServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Printf("metrics server listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics: %v", err)
		}
	}()
	return srv
}
