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

	iface "github.com/ipfs/boxo/coreiface"
	"github.com/ipfs/boxo/coreiface/options"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/coreapi"
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
			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}

		err := r.ParseMultipartForm(2 << 30) // 2GB limit
		if err != nil {
			http.Error(w, "Couldn't parse multipart form", http.StatusBadRequest)
			return
		}

		fileHeader := r.MultipartForm.File["file"]
		if len(fileHeader) == 0 {
			http.Error(w, "No file provided", http.StatusBadRequest)
			return
		}

		file, err := fileHeader[0].Open()
		if err != nil {
			http.Error(w, "Couldn't open file", http.StatusInternalServerError)
			return
		}
		defer file.Close()

		data, err := io.ReadAll(file)
		if err != nil {
			http.Error(w, "Unable to read the file", http.StatusBadRequest)
			return
		}

		// TODO: optionally push to an IPFS node for actual p2p sharing

		ctx := context.Background()
		cidFile, err := api.Unixfs().Add(ctx, files.NewBytesFile(data), options.Unixfs.Pin(false))
		if err != nil {
			http.Error(w, "Failed to add file", http.StatusInternalServerError)
			return
		}

		err = saveFileToRenterd(data, cidFile.Cid().String(), cfg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, "CID: %s\n", cidFile.Cid())
		fmt.Fprintf(w, "%s/ipfs/%s\n", cfg.Domain, cidFile.Cid())
	}
}

func downloadHandler(cfg *Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cid := filepath.Base(r.URL.Path)

		req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/worker/objects/%s?bucket=%s", cfg.RenterdURL, cid, cfg.RenterdBucket), nil)
		if err != nil {
			http.Error(w, "Failed to create request", http.StatusInternalServerError)
			return
		}

		authValue := "Basic " + base64.StdEncoding.EncodeToString([]byte(":"+cfg.RenterdPassword))
		req.Header.Set("Authorization", authValue)
		req.Header.Set("Range", r.Header.Get("Range"))

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		for key, values := range resp.Header {
			for _, value := range values {
				w.Header().Set(key, value)
			}
		}

		if r.Header.Get("Range") != "" {
			w.WriteHeader(http.StatusPartialContent)
		}

		io.Copy(w, resp.Body)
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
		return fmt.Errorf("failed to save file to renterd: %s", resp.Status)
	}
	defer resp.Body.Close()

	return nil
}

func basicAuth(handler http.HandlerFunc, password string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "" || pass != password {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		handler(w, r)
	}
}

func main() {
	ctx := context.Background()
	cfg, err := readConfig()
	if err != nil {
		fmt.Println("Failed to read config:", err)
		return
	}
	fmt.Printf("renterd:\t%s\n", cfg.RenterdURL)
	fmt.Printf("bucket:\t\t%s\n", cfg.RenterdBucket)
	node, err := core.NewNode(ctx, &core.BuildCfg{
		Online: false,
	})
	if err != nil {
		fmt.Println("Failed to start IPFS node:", err)
		return
	}
	defer node.Close()

	api, err := coreapi.NewCoreAPI(node)
	if err != nil {
		fmt.Println("Failed to create core API")
		return
	}

	http.HandleFunc("/upload", basicAuth(uploadHandler(api, cfg), cfg.UploadPassword))
	http.HandleFunc("/ipfs/", downloadHandler(cfg))

	server := &http.Server{Addr: ":8080"}

	go func() {
		fmt.Println("Server running on :8080")
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Printf("ListenAndServe(): %s\n", err)
		}
	}()

	// Graceful shutdown on Ctrl+C
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	<-stop
	fmt.Println("\nShutting down server...")
	if err := server.Shutdown(ctx); err != nil {
		fmt.Printf("Error shutting down: %s\n", err)
	}
}
