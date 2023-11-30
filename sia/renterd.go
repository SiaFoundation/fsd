package sia

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"go.sia.tech/fsd/config"
)

// downloadPartialData range requests in the worker client are broken
func downloadPartialData(ctx context.Context, cfg config.Renterd, bucket, key string, offset, length uint64) ([]byte, error) {
	u, err := url.Parse(cfg.WorkerAddress + "/objects/" + key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url: %w", err)
	}
	p := url.Values{
		"bucket": []string{bucket},
	}
	u.RawQuery = p.Encode()

	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.SetBasicAuth("", cfg.WorkerPassword)
	req.Header.Set("Range", fmt.Sprintf("bytes=%v-%v", offset, offset+length-1))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to download object: %w", err)
	} else if resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("failed to download object: %s", resp.Status)
	}

	lr := io.LimitReader(resp.Body, int64(length))
	buf, err := io.ReadAll(lr)
	if err != nil {
		return nil, fmt.Errorf("failed to download object: %w", err)
	}
	return buf, nil
}
