package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"go.sia.tech/siapfs/config"
)

func downloadObject(ctx context.Context, renterd config.Renterd, key string, offset, length uint64) (io.ReadCloser, error) {
	values := url.Values{}
	values.Set("bucket", url.QueryEscape(renterd.Bucket))
	url := fmt.Sprintf("%s/objects/%s?%s", renterd.Address, key, values.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth("", renterd.Password)
	req.Header.Add("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		err, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return nil, errors.New(string(err))
	}
	return resp.Body, err
}
