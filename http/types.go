package http

import "time"

// The ProviderStatsResp struct is used to return stats about the reprovider
// system.
type ProviderStatsResp struct {
	TotalProvides          uint64        `json:"totalProvides"`
	LastReprovideBatchSize uint64        `json:"lastReprovideBatchSize"`
	AvgProvideDuration     time.Duration `json:"avgProvideDuration"`
	LastReprovideDuration  time.Duration `json:"lastReprovideDuration"`
}
