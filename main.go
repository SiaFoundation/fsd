package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	iface "github.com/ipfs/boxo/coreiface"
	"github.com/ipfs/boxo/coreiface/options"
	"github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/coreapi"
	"github.com/libp2p/go-libp2p/core/host"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Domain          string `yaml:"domain"`
	UploadPassword  string `yaml:"upload_password"`
	RenterdURL      string `yaml:"renterd_url"`
	RenterdPassword string `yaml:"renterd_password"`
	RenterdBucket   string `yaml:"renterd_bucket"`
}

func readConfig() (*Config, error) {
	cfg := Config{}
	data, err := os.ReadFile("config.yml")
	if err != nil {
		return &cfg, err
	}
	err = yaml.Unmarshal(data, &cfg)
	return &cfg, err
}

func uploadHandler(api iface.CoreAPI, cfg *Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "only POST allowed", http.StatusMethodNotAllowed)
			return
		}

		err := r.ParseMultipartForm(2 << 30) // 2GB limit
		if err != nil {
			http.Error(w, "couldn't parse multipart form", http.StatusBadRequest)
			return
		}

		fileHeader := r.MultipartForm.File["file"]
		if len(fileHeader) == 0 {
			http.Error(w, "no file provided", http.StatusBadRequest)
			return
		}

		file, err := fileHeader[0].Open()
		if err != nil {
			http.Error(w, "couldn't open file", http.StatusInternalServerError)
			return
		}
		defer file.Close()

		data, err := io.ReadAll(file)
		if err != nil {
			http.Error(w, "unable to read the file", http.StatusBadRequest)
			return
		}

		// TODO: optionally push to an IPFS node for actual p2p sharing

		ctx := context.Background()
		cidFile, err := api.Unixfs().Add(ctx, files.NewBytesFile(data), options.Unixfs.Pin(false))
		if err != nil {
			http.Error(w, "failed to add file", http.StatusInternalServerError)
			return
		}

		err = saveFileToRenterd(data, cidFile.Cid().String(), cfg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "CID: %s\n", cidFile.Cid())
		fmt.Fprintf(w, "URL: %s/ipfs/%s\n", cfg.Domain, cidFile.Cid())
	}
}

func downloadHandler(api iface.CoreAPI, cfg *Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cid := filepath.Base(r.URL.Path)

		// Try to fetch the file from renterd
		req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/worker/objects/%s?bucket=%s", cfg.RenterdURL, cid, cfg.RenterdBucket), nil)
		if err != nil {
			http.Error(w, "failed to create request", http.StatusInternalServerError)
			return
		}

		authValue := "Basic " + base64.StdEncoding.EncodeToString([]byte(":"+cfg.RenterdPassword))
		req.Header.Set("Authorization", authValue)
		req.Header.Set("Range", r.Header.Get("Range"))

		client := &http.Client{}
		resp, err := client.Do(req)

		if err == nil && resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
			// If the file was found in renterd, serve it to the client
			defer resp.Body.Close()

			for key, values := range resp.Header {
				for _, value := range values {
					w.Header().Set(key, value)
				}
			}

			if r.Header.Get("Range") != "" {
				w.WriteHeader(http.StatusPartialContent)
			}

			fmt.Println("retreived from renterd: ", cid)
			io.Copy(w, resp.Body)
			return
		}

		// If the file wasn't found in renterd or there was an error, try fetching from IPFS
		ctx := context.Background()
		// TODO: fetch from IPFS with range requests
		data, err := api.Unixfs().Get(ctx, path.New(cid))
		if err != nil {
			message := fmt.Sprintf("failed to retrieve from IPFS: %s", cid)
			fmt.Println(message)
			http.Error(w, message, http.StatusInternalServerError)
			return
		}

		switch f := data.(type) {
		case files.File:
			bytes, err := io.ReadAll(f)
			go func() {
				if err != nil {
					fmt.Println("failed to pin to renterd: ", cid)
					return
				}
				err = saveFileToRenterd(bytes, cid, cfg)
				if err != nil {
					fmt.Println("failed to pin to renterd: ", cid)
					return
				}
				fmt.Println("pinned to renterd: ", cid)
			}()
			w.Write(bytes)
		case files.Directory:
			message := fmt.Sprintf("resolves to a directory, not a file: %s", cid)
			fmt.Println(message)
			http.Error(w, message, http.StatusBadRequest)
			return
		default:
			message := fmt.Sprintf("unknown data type: %s", cid)
			fmt.Println(message)
			http.Error(w, message, http.StatusInternalServerError)
			return
		}
	}
}

func saveFileToRenterd(data []byte, cid string, cfg *Config) error {
	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/api/worker/objects/%s?bucket=%s", cfg.RenterdURL, cid, cfg.RenterdBucket), bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	authValue := "Basic " + base64.StdEncoding.EncodeToString([]byte(":"+cfg.RenterdPassword))
	fmt.Println(authValue)
	req.Header.Set("Authorization", authValue)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to save to renterd: %s", resp.Status)
	}
	defer resp.Body.Close()

	return nil
}

func basicAuth(handler http.HandlerFunc, password string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "" || pass != password {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		handler(w, r)
	}
}

func printConnectedPeersEveryMinute(ctx context.Context, h host.Host) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			peerInfos := h.Network().Peers()
			fmt.Printf("Connected to %d peers.\n", len(peerInfos))
			// for _, peerInfo := range peerInfos {
			// 	fmt.Println(peerInfo)
			// }
		case <-ctx.Done():
			return
		}
	}
}

func main() {
	ctx := context.Background()
	cfg, err := readConfig()
	if err != nil {
		fmt.Println("failed to read config:", err)
		return
	}
	fmt.Printf("renterd:\t%s\n", cfg.RenterdURL)
	fmt.Printf("bucket:\t\t%s\n", cfg.RenterdBucket)
	node, err := core.NewNode(ctx, &core.BuildCfg{
		Online: true,
	})
	if err != nil {
		fmt.Println("failed to start IPFS node:", err)
		return
	}
	defer node.Close()

	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		fmt.Println("failed to create core API")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go printConnectedPeersEveryMinute(ctx, node.PeerHost)

	http.HandleFunc("/upload", basicAuth(uploadHandler(api, cfg), cfg.UploadPassword))
	http.HandleFunc("/ipfs/", downloadHandler(api, cfg))

	server := &http.Server{Addr: ":8080"}

	go func() {
		fmt.Println("server running on :8080")
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Printf("ListenAndServe(): %s\n", err)
		}
	}()

	// Graceful shutdown on Ctrl+C
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	<-stop
	fmt.Println("\nshutting down server...")
	if err := server.Shutdown(ctx); err != nil {
		fmt.Printf("error shutting down: %s\n", err)
	}
}
