package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

func downloadObject(ctx context.Context, address, password, bucket, key string, offset, length uint64) (io.ReadCloser, error) {
	values := url.Values{}
	values.Set("bucket", url.QueryEscape(bucket))
	url := fmt.Sprintf("%s/objects/%s?%s", address, key, values.Encode())

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth("", password)
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
