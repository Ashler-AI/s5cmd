package command

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/progressbar"
	"github.com/peak/s5cmd/v2/storage"
	storageurl "github.com/peak/s5cmd/v2/storage/url"
	"github.com/urfave/cli/v2"
)

func TestExactSizeWriterAtRejectsOverflow(t *testing.T) {
	destination := newMemoryWriterAt(8)
	writer := newExactSizeWriterAt(destination, 8)

	if _, err := writer.WriteAt([]byte("1234"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteAt([]byte("5678"), 4); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteAt([]byte("9"), 8); !errors.Is(err, errDownloadExceedsExpectedSize) {
		t.Fatalf("overflow error = %v, want %v", err, errDownloadExceedsExpectedSize)
	}
	if got := string(destination.data); got != "12345678" {
		t.Fatalf("destination = %q, want exact bounded contents", got)
	}
}

func TestValidateDownloadedSizeRejectsShortRead(t *testing.T) {
	if err := validateDownloadedSize(7, 8); !errors.Is(err, errDownloadSizeMismatch) {
		t.Fatalf("short-read error = %v, want %v", err, errDownloadSizeMismatch)
	}
	if err := validateDownloadedSize(8, 8); err != nil {
		t.Fatalf("exact size rejected: %v", err)
	}
}

func TestDownloadExactSizeGuardRemovesInvalidOutput(t *testing.T) {
	log.Init("error", false)

	testcases := []struct {
		name     string
		expected int64
		wantErr  error
	}{
		{name: "exact", expected: 4},
		{name: "overflow", expected: 3, wantErr: errDownloadExceedsExpectedSize},
		{name: "short read", expected: 5, wantErr: errDownloadSizeMismatch},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", "4")
				w.Header().Set("ETag", `"etag"`)
				_, _ = w.Write([]byte("four"))
			}))
			defer server.Close()

			src, err := storageurl.New("s3://bucket/object")
			if err != nil {
				t.Fatal(err)
			}
			destination := filepath.Join(t.TempDir(), "object")
			dst, err := storageurl.New(destination)
			if err != nil {
				t.Fatal(err)
			}
			copyCommand := Copy{
				op:                   "cp",
				fullCommand:          "cp",
				concurrency:          1,
				partSize:             5 * 1024 * 1024,
				progressbar:          &progressbar.NoOp{},
				maxDownloadBytes:     tc.expected,
				enforceDownloadBytes: true,
				storageOpts: storage.Options{
					Endpoint:      server.URL,
					NoSignRequest: true,
					MaxRetries:    0,
				},
			}

			err = copyCommand.doDownload(context.Background(), src, dst)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("exact download failed: %v", err)
				}
				got, readErr := os.ReadFile(destination)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if string(got) != "four" {
					t.Fatalf("destination = %q, want four", got)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("download error = %v, want %v", err, tc.wantErr)
			}
			if _, statErr := os.Stat(destination); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("invalid destination was retained: %v", statErr)
			}
		})
	}
}

func TestValidateCopyAllowsExactGCSGenerationDownload(t *testing.T) {
	ctx := newCommandValidationContext(t, NewCopyCommand(),
		[]string{"--version-id", "1700000000000001", "--max-download-bytes", "8", "--raw", "s3://bucket/object[1]", "output[1]"},
		"https://storage.googleapis.com",
	)
	if err := validateCopyCommand(ctx); err != nil {
		t.Fatalf("exact GCS generation download rejected: %v", err)
	}
}

func TestValidateCopyRejectsUnsupportedGCSVersionOperations(t *testing.T) {
	testcases := []struct {
		name string
		args []string
	}{
		{
			name: "server-side copy",
			args: []string{"--version-id", "7", "s3://bucket/source", "s3://bucket/destination"},
		},
		{
			name: "wildcard download",
			args: []string{"--version-id", "7", "s3://bucket/*", "destination/"},
		},
		{
			name: "upload",
			args: []string{"--version-id", "7", "source", "s3://bucket/destination"},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newCommandValidationContext(t, NewCopyCommand(), tc.args, "https://storage.googleapis.com")
			if err := validateCopyCommand(ctx); err == nil {
				t.Fatal("unsupported GCS version operation was accepted")
			}
		})
	}
}

func TestValidateMoveRejectsGCSGenerationDownload(t *testing.T) {
	ctx := newCommandValidationContext(t, NewMoveCommand(),
		[]string{"--version-id", "7", "s3://bucket/source", "destination"},
		"https://storage.googleapis.com",
	)
	if err := validateCopyCommand(ctx); err == nil {
		t.Fatal("mutating GCS generation download was accepted")
	}
}

func TestValidateMaxDownloadBytesRequiresExactDownload(t *testing.T) {
	testcases := []struct {
		name string
		args []string
	}{
		{
			name: "negative bound",
			args: []string{"--max-download-bytes", "-1", "s3://bucket/object", "destination"},
		},
		{
			name: "wildcard source",
			args: []string{"--max-download-bytes", "8", "s3://bucket/*", "destination/"},
		},
		{
			name: "upload",
			args: []string{"--max-download-bytes", "8", "source", "s3://bucket/object"},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newCommandValidationContext(t, NewCopyCommand(), tc.args, "")
			if err := validateCopyCommand(ctx); err == nil {
				t.Fatal("invalid exact-size download guard was accepted")
			}
		})
	}
}

func TestGCSGenerationValidationForReadCommands(t *testing.T) {
	testcases := []struct {
		name     string
		command  *cli.Command
		args     []string
		validate func(*cli.Context) error
	}{
		{
			name:     "cat",
			command:  NewCatCommand(),
			args:     []string{"--version-id", "7", "s3://bucket/object"},
			validate: validateCatCommand,
		},
		{
			name:     "head",
			command:  NewHeadCommand(),
			args:     []string{"--version-id", "7", "s3://bucket/object"},
			validate: validateHeadCommand,
		},
		{
			name:     "presign",
			command:  NewPresignCommand(),
			args:     []string{"--version-id", "7", "s3://bucket/object"},
			validate: validatePresignCommand,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newCommandValidationContext(t, tc.command, tc.args, "https://storage.googleapis.com")
			if err := tc.validate(ctx); err != nil {
				t.Fatalf("exact GCS generation read rejected: %v", err)
			}
		})
	}
}

func TestGCSVersioningDefaultRemainsRejected(t *testing.T) {
	ctx := newCommandValidationContext(t, NewDeleteCommand(),
		[]string{"--version-id", "7", "s3://bucket/object"},
		"https://storage.googleapis.com",
	)
	if err := checkVersioningWithGoogleEndpoint(ctx); err == nil {
		t.Fatal("mutating GCS version operation was accepted")
	}
}

func newCommandValidationContext(t *testing.T, command *cli.Command, args []string, endpoint string) *cli.Context {
	t.Helper()

	globalSet := flag.NewFlagSet("s5cmd", flag.ContinueOnError)
	for _, appFlag := range app.Flags {
		if err := appFlag.Apply(globalSet); err != nil {
			t.Fatal(err)
		}
	}
	if endpoint != "" {
		if err := globalSet.Parse([]string{"--endpoint-url", endpoint}); err != nil {
			t.Fatal(err)
		}
	}
	parent := cli.NewContext(app, globalSet, nil)

	commandSet := flag.NewFlagSet(command.Name, flag.ContinueOnError)
	for _, commandFlag := range command.Flags {
		if err := commandFlag.Apply(commandSet); err != nil {
			t.Fatal(err)
		}
	}
	if err := commandSet.Parse(args); err != nil {
		t.Fatal(err)
	}
	ctx := cli.NewContext(app, commandSet, parent)
	ctx.Command = command
	return ctx
}

type memoryWriterAt struct {
	data []byte
}

func newMemoryWriterAt(size int) *memoryWriterAt {
	return &memoryWriterAt{data: bytes.Repeat([]byte{0}, size)}
}

func (w *memoryWriterAt) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(w.data)) && len(p) > 0 {
		return 0, io.ErrShortWrite
	}
	n := copy(w.data[off:], p)
	if n != len(p) {
		return n, io.ErrShortWrite
	}
	return n, nil
}
