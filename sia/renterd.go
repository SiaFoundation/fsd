package sia

import (
	"fmt"
	"io"
	"net/http"
	"net/url"

	"go.sia.tech/fsd/config"
)

// downloadPartialData range requests in the worker client are broken
func downloadPartialData(cfg config.Renterd, key string, offset, length uint64) ([]byte, error) {
	u, err := url.Parse(cfg.Address + "/objects/" + key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url: %w", err)
	}
	p := url.Values{
		"bucket": []string{cfg.Bucket},
	}
	u.RawQuery = p.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.SetBasicAuth("", cfg.Password)
	req.Header.Add("Range", fmt.Sprintf("bytes=%v-%v", offset, offset+length-1))

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
