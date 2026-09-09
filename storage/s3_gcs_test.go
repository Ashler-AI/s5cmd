package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	urlpkg "net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	storageurl "github.com/peak/s5cmd/v2/storage/url"
)

func newGCSTestSession(t *testing.T, endpoint string, maxRetries int) *session.Session {
	t.Helper()

	sess, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials("access-key", "secret-key", ""),
		Endpoint:         aws.String(endpoint),
		Region:           aws.String("us-east-1"),
		S3ForcePathStyle: aws.Bool(true),
		MaxRetries:       aws.Int(maxRetries),
		SleepDelay:       func(time.Duration) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	addGCSCompatibilityHandlers(sess)
	return sess
}

func TestGCSGenerationPinnedMultipartDownload(t *testing.T) {
	const generation = "1700000000000001"
	const partSize = 5 * 1024 * 1024

	gen1 := bytes.Repeat([]byte("generation-one\n"), 700000)
	gen2 := bytes.Repeat([]byte("generation-two\n"), 700000)
	if len(gen1) <= 2*partSize {
		t.Fatalf("test object must span multiple parts, got %d bytes", len(gen1))
	}

	var (
		mu       sync.Mutex
		requests int
		ranges   []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		ranges = append(ranges, r.Header.Get("Range"))
		mu.Unlock()

		if got := r.URL.Query().Get("generation"); got != generation {
			t.Errorf("generation query = %q, want %q", got, generation)
		}
		if got := r.URL.Query().Get("versionId"); got != "" {
			t.Errorf("versionId query must be removed for GCS, got %q", got)
		}
		if got := r.Header.Get("Accept-Encoding"); got != "gzip" {
			t.Errorf("Accept-Encoding = %q, want gzip", got)
		}

		body := gen2 // an unpinned request observes the overwritten generation
		if r.URL.Query().Get("generation") == generation {
			body = gen1
		}

		start, end := int64(0), int64(len(body)-1)
		if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
			var err error
			start, end, err = parseTestByteRange(rangeHeader, int64(len(body)))
			if err != nil {
				t.Error(err)
				http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10)+"/"+strconv.Itoa(len(body)))
			w.WriteHeader(http.StatusPartialContent)
		}
		_, _ = w.Write(body[start : end+1])
	}))
	defer server.Close()

	sess := newGCSTestSession(t, server.URL, 0)
	client := &S3{
		api:         s3.New(sess),
		downloader:  s3manager.NewDownloader(sess),
		endpointURL: mustParseTestURL(t, "https://"+gcsEndpoint),
	}
	src, err := storageurl.New("s3://bucket/object", storageurl.WithVersion(generation))
	if err != nil {
		t.Fatal(err)
	}

	writer := &testWriterAt{data: make([]byte, len(gen1))}
	n, err := client.Get(context.Background(), src, writer, 3, partSize)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(gen1)) {
		t.Fatalf("downloaded bytes = %d, want %d", n, len(gen1))
	}
	if !bytes.Equal(writer.data, gen1) {
		t.Fatal("download mixed or selected the wrong GCS generation")
	}

	mu.Lock()
	defer mu.Unlock()
	if requests < 3 {
		t.Fatalf("requests = %d, want at least 3 multipart GETs", requests)
	}
	for _, rangeHeader := range ranges {
		if rangeHeader == "" {
			t.Fatalf("multipart request missing Range header: %v", ranges)
		}
	}
}

func TestGCSGenerationPinnedHead(t *testing.T) {
	const generation = "1700000000000001"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("method = %s, want HEAD", r.Method)
			return
		}
		if got := r.URL.Query().Get("generation"); got != generation {
			t.Errorf("generation query = %q, want %q", got, generation)
		}
		if got := r.URL.Query().Get("versionId"); got != "" {
			t.Errorf("versionId query must be removed for GCS, got %q", got)
		}
		if got := r.Header.Get("Accept-Encoding"); got != "gzip" {
			t.Errorf("Accept-Encoding = %q, want gzip", got)
		}
		w.Header().Set("Content-Length", "12")
		w.Header().Set("ETag", `"etag"`)
	}))
	defer server.Close()

	sess := newGCSTestSession(t, server.URL, 0)
	client := &S3{api: s3.New(sess)}
	src, err := storageurl.New("s3://bucket/object", storageurl.WithVersion(generation))
	if err != nil {
		t.Fatal(err)
	}
	obj, err := client.Stat(context.Background(), src)
	if err != nil {
		t.Fatal(err)
	}
	if obj.Size != 12 {
		t.Fatalf("size = %d, want 12", obj.Size)
	}
}

func TestNewSessionInstallsGCSCompatibility(t *testing.T) {
	globalSessionCache.clear()
	t.Cleanup(globalSessionCache.clear)

	sess, err := globalSessionCache.newSession(context.Background(), Options{
		Endpoint:      "https://" + gcsEndpoint,
		NoSignRequest: true,
		region:        "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := s3.New(sess).GetObjectRequest(&s3.GetObjectInput{
		Bucket:    aws.String("bucket"),
		Key:       aws.String("object"),
		VersionId: aws.String("55"),
	})
	if err := req.Build(); err != nil {
		t.Fatal(err)
	}
	if err := req.Sign(); err != nil {
		t.Fatal(err)
	}
	if got := req.HTTPRequest.URL.Query().Get("generation"); got != "55" {
		t.Fatalf("generation query = %q, want 55", got)
	}
}

func TestGCSPreservesStoredGzipBytes(t *testing.T) {
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	_, _ = zw.Write([]byte("the uncompressed object body"))
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Accept-Encoding"); got != "gzip" {
			t.Errorf("Accept-Encoding = %q, want gzip", got)
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", strconv.Itoa(compressed.Len()))
		_, _ = w.Write(compressed.Bytes())
	}))
	defer server.Close()

	sess := newGCSTestSession(t, server.URL, 0)
	client := &S3{api: s3.New(sess)}
	src, err := storageurl.New("s3://bucket/object", storageurl.WithVersion("42"))
	if err != nil {
		t.Fatal(err)
	}

	reader, err := client.Read(context.Background(), src)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, compressed.Bytes()) {
		t.Fatalf("downloaded body was transparently decoded: got %d bytes, want %d stored bytes", len(got), compressed.Len())
	}
}

func TestGCSGenerationSurvivesRetry(t *testing.T) {
	const generation = "99"
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if got := r.URL.Query().Get("generation"); got != generation {
			t.Errorf("request %d generation = %q, want %q", requests, got, generation)
		}
		if requests == 1 {
			http.Error(w, "retry", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte("generation-99"))
	}))
	defer server.Close()

	sess := newGCSTestSession(t, server.URL, 1)
	client := &S3{api: s3.New(sess)}
	src, err := storageurl.New("s3://bucket/object", storageurl.WithVersion(generation))
	if err != nil {
		t.Fatal(err)
	}

	reader, err := client.Read(context.Background(), src)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "generation-99" {
		t.Fatalf("body = %q, want generation-99", got)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}

func TestGCSDownloadHonorsCancellation(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()

	sess := newGCSTestSession(t, server.URL, 0)
	client := &S3{api: s3.New(sess)}
	src, err := storageurl.New("s3://bucket/object", storageurl.WithVersion("7"))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := client.Read(ctx, src)
		errCh <- err
	}()
	<-started
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), context.Canceled.Error()) {
			t.Fatalf("error = %v, want context cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("download did not stop after cancellation")
	}
}

func TestGCSGenerationIsSignedAndAWSVersionIDIsUnchanged(t *testing.T) {
	const generation = "123456789"
	fixedTime := time.Unix(1700000000, 0).UTC()

	gcsSession := newGCSTestSession(t, "https://"+gcsEndpoint, 0)
	gcsClient := s3.New(gcsSession)
	gcsRequest, _ := gcsClient.GetObjectRequest(&s3.GetObjectInput{
		Bucket:    aws.String("bucket"),
		Key:       aws.String("object"),
		VersionId: aws.String(generation),
	})
	gcsRequest.Time = fixedTime
	gcsURL, err := gcsRequest.Presign(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	gcsParsed := mustParseTestURL(t, gcsURL)
	if got := gcsParsed.Query().Get("X-Amz-SignedHeaders"); got != "host" {
		t.Fatalf("standalone presigned URL requires undisclosed headers: %q", got)
	}
	if got := gcsParsed.Query().Get("generation"); got != generation {
		t.Fatalf("presigned generation = %q, want %q", got, generation)
	}
	if got := gcsParsed.Query().Get("versionId"); got != "" {
		t.Fatalf("presigned GCS URL retained versionId = %q", got)
	}

	otherRequest, _ := gcsClient.GetObjectRequest(&s3.GetObjectInput{
		Bucket:    aws.String("bucket"),
		Key:       aws.String("object"),
		VersionId: aws.String(generation + "0"),
	})
	otherRequest.Time = fixedTime
	otherURL, err := otherRequest.Presign(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	otherParsed := mustParseTestURL(t, otherURL)
	if gcsParsed.Query().Get("X-Amz-Signature") == otherParsed.Query().Get("X-Amz-Signature") {
		t.Fatal("presigned signature did not change when the GCS generation changed")
	}

	awsSession, err := session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials("access-key", "secret-key", ""),
		Region:      aws.String("us-east-1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	awsRequest, _ := s3.New(awsSession).GetObjectRequest(&s3.GetObjectInput{
		Bucket:    aws.String("bucket"),
		Key:       aws.String("object"),
		VersionId: aws.String(generation),
	})
	if err := awsRequest.Build(); err != nil {
		t.Fatal(err)
	}
	if got := awsRequest.HTTPRequest.URL.Query().Get("versionId"); got != generation {
		t.Fatalf("AWS versionId = %q, want %q", got, generation)
	}
	if got := awsRequest.HTTPRequest.URL.Query().Get("generation"); got != "" {
		t.Fatalf("AWS request unexpectedly gained generation = %q", got)
	}
}

func TestGCSControlRequestsRetainEncodingBehavior(t *testing.T) {
	sess := newGCSTestSession(t, "https://"+gcsEndpoint, 0)
	req, _ := s3.New(sess).ListObjectsV2Request(&s3.ListObjectsV2Input{Bucket: aws.String("bucket")})
	if err := req.Sign(); err != nil {
		t.Fatal(err)
	}
	if got := req.HTTPRequest.Header.Get("Accept-Encoding"); got != "" {
		t.Fatalf("object encoding policy leaked to list request: %q", got)
	}
}

type testWriterAt struct {
	mu   sync.Mutex
	data []byte
}

func (w *testWriterAt) WriteAt(p []byte, off int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return copy(w.data[off:], p), nil
}

func parseTestByteRange(header string, size int64) (int64, int64, error) {
	value := strings.TrimPrefix(header, "bytes=")
	parts := strings.SplitN(value, "-", 2)
	if len(parts) != 2 {
		return 0, 0, errors.New("invalid Range header: " + header)
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	end := size - 1
	if parts[1] != "" {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, err
		}
	}
	if start < 0 || start >= size || end < start {
		return 0, 0, errors.New("range outside object: " + header)
	}
	if end >= size {
		end = size - 1
	}
	return start, end, nil
}

func mustParseTestURL(t *testing.T, rawURL string) urlpkg.URL {
	t.Helper()
	u, err := urlpkg.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	return *u
}
