package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"golang.org/x/crypto/ssh"
	"libvirt.org/go/libvirt"
)

const testRepoRootQCOW2 = "flatcar_production_qemu_image.img" // This is a qcow2 image - download it from https://stable.release.flatcar-linux.net/amd64-usr/current/flatcar_production_qemu_image.img

type repoRootPool struct {
	Name          string
	Dir           string
	BaseImagePath string
}

func TestInstanceGroupInitWithRepoRootQCOW2Path(t *testing.T) {
	t.Helper()

	conn := requireSystemLibvirt(t)
	defer closeConnect(conn)

	repoRoot := repoRootDir(t)
	requireDefaultNetwork(t, conn)
	pool := createRepoRootPool(t, conn, repoRoot)

	group := &InstanceGroup{
		URI:            defaultURI,
		PoolName:       pool.Name,
		BaseVolumePath: pool.BaseImagePath,
		NetworkName:    defaultNetworkName,
		StateDir:       t.TempDir(),
		DomainPrefix:   "provider-test",
		MaxSize:        1,
	}

	info, err := group.Init(context.Background(), hclog.NewNullLogger(), provider.Settings{
		ConnectorConfig: provider.ConnectorConfig{
			OS:       "linux",
			Arch:     "amd64",
			Protocol: provider.ProtocolSSH,
			Username: "core",
			Password: "test-password",
		},
	})
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	if info.MaxSize != group.MaxSize {
		t.Fatalf("Init() max size = %d, want %d", info.MaxSize, group.MaxSize)
	}
	if !strings.Contains(info.ID, group.DomainPrefix) {
		t.Fatalf("Init() provider ID = %q, want prefix %q", info.ID, group.DomainPrefix)
	}
	if group.passwordHash == "" {
		t.Fatal("Init() did not create an ignition password hash")
	}
}

func TestResolveBaseVolumeAndCreateOverlayRepoRootQCOW2(t *testing.T) {
	t.Helper()

	conn := requireSystemLibvirt(t)
	defer closeConnect(conn)

	repoRoot := repoRootDir(t)
	requireRepoRootQCOW2(t, repoRoot)
	poolRef := createRepoRootPool(t, conn, repoRoot)

	pool, err := conn.LookupStoragePoolByName(poolRef.Name)
	if err != nil {
		t.Fatalf("LookupStoragePoolByName(%q) error = %v", poolRef.Name, err)
	}
	defer pool.Free()

	group := &InstanceGroup{
		PoolName:       poolRef.Name,
		BaseVolumeName: testRepoRootQCOW2,
	}

	baseVol, details, err := group.resolveBaseVolume(conn, pool)
	if err != nil {
		t.Fatalf("resolveBaseVolume() error = %v", err)
	}
	defer baseVol.Free()

	if details.Format != "qcow2" {
		t.Fatalf("resolveBaseVolume() format = %q, want %q", details.Format, "qcow2")
	}
	if details.Capacity == 0 {
		t.Fatal("resolveBaseVolume() reported zero capacity")
	}
	if details.Path == "" {
		t.Fatal("resolveBaseVolume() reported empty path")
	}

	overlayName := fmt.Sprintf("provider-test-overlay-%d.qcow2", time.Now().UnixNano())
	overlay, err := pool.StorageVolCreateXML(renderVolumeOverlayXML(overlayName, details), 0)
	if err != nil {
		t.Fatalf("StorageVolCreateXML() error = %v", err)
	}
	defer func() {
		_ = overlay.Delete(0)
		_ = overlay.Free()
	}()

	desc, err := overlay.GetXMLDesc(0)
	if err != nil {
		t.Fatalf("overlay.GetXMLDesc() error = %v", err)
	}
	if got := volumeFormatFromXML(desc); got != "qcow2" {
		t.Fatalf("overlay format = %q, want %q", got, "qcow2")
	}
	if got := volumeBackingStorePathFromXML(desc); got != details.Path {
		t.Fatalf("overlay backingStore path = %q, want %q", got, details.Path)
	}

	overlayPath, err := overlay.GetPath()
	if err != nil {
		t.Fatalf("overlay.GetPath() error = %v", err)
	}
	if want := filepath.Join(poolRef.Dir, overlayName); overlayPath != want {
		t.Fatalf("overlay path = %q, want %q", overlayPath, want)
	}
}

func TestInstanceGroupLifecycleBootsFlatcarAndDockerIsReachable(t *testing.T) {
	t.Helper()

	conn := requireSystemLibvirt(t)
	defer closeConnect(conn)

	repoRoot := repoRootDir(t)
	requireRepoRootQCOW2(t, repoRoot)
	requireDefaultNetwork(t, conn)
	poolRef := createRepoRootPool(t, conn, repoRoot)
	stateDir := createAccessibleStateDir(t, repoRoot)
	privateKeyPEM := generateSSHPrivateKeyPEM(t)

	group := &InstanceGroup{
		URI:            defaultURI,
		PoolName:       poolRef.Name,
		BaseVolumeName: testRepoRootQCOW2,
		NetworkName:    defaultNetworkName,
		StateDir:       stateDir,
		DomainPrefix:   fmt.Sprintf("provider-full-%d", time.Now().UnixNano()),
		MaxSize:        1,
		VCPUCount:      2,
		MemoryMiB:      2048,
		AddressSource:  "lease",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	info, err := group.Init(ctx, hclog.NewNullLogger(), provider.Settings{
		ConnectorConfig: provider.ConnectorConfig{
			OS:       "linux",
			Arch:     "amd64",
			Protocol: provider.ProtocolSSH,
			Username: "core",
			Password: "test-password",
			Key:      privateKeyPEM,
		},
	})
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	if info.MaxSize != 1 {
		t.Fatalf("Init() max size = %d, want 1", info.MaxSize)
	}

	var instanceID string
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cleanupCancel()

		if instanceID != "" {
			_, _ = group.Decrease(cleanupCtx, []string{instanceID})
		}
		_ = group.Shutdown(cleanupCtx)
	})

	created, err := group.Increase(ctx, 1)
	if err != nil {
		t.Fatalf("Increase() error = %v", err)
	}
	if created != 1 {
		t.Fatalf("Increase() created = %d, want 1", created)
	}

	instanceID, err = waitForManagedInstanceState(ctx, group, provider.StateRunning)
	if err != nil {
		t.Fatalf("waitForManagedInstanceState() error = %v", err)
	}

	connectInfo, err := waitForConnectInfo(ctx, group, instanceID)
	if err != nil {
		t.Fatalf("waitForConnectInfo() error = %v", err)
	}
	if connectInfo.ExternalAddr == "" {
		t.Fatal("ConnectInfo() returned an empty ExternalAddr")
	}
	if err := waitForHeartbeat(ctx, group, instanceID); err != nil {
		t.Fatalf("waitForHeartbeat() error = %v", err)
	}

	if output, err := waitForSSHCommand(
		ctx,
		connectInfo,
		privateKeyPEM,
		"sh -lc 'docker version >/dev/null && systemctl is-active docker'",
	); err != nil {
		t.Fatalf("docker readiness check failed: %v\noutput:\n%s", err, output)
	}

	removed, err := group.Decrease(ctx, []string{instanceID})
	if err != nil {
		t.Fatalf("Decrease() error = %v", err)
	}
	if len(removed) != 1 || removed[0] != instanceID {
		t.Fatalf("Decrease() removed = %v, want [%s]", removed, instanceID)
	}

	if err := waitForInstanceRemoved(ctx, group, instanceID); err != nil {
		t.Fatalf("waitForInstanceRemoved() error = %v", err)
	}
	instanceID = ""

	if err := group.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestInstanceGroupShutdownRemovesAllManagedVMs(t *testing.T) {
	conn := requireSystemLibvirt(t)
	defer closeConnect(conn)

	repoRoot := repoRootDir(t)
	requireRepoRootQCOW2(t, repoRoot)
	requireDefaultNetwork(t, conn)
	poolRef := createRepoRootPool(t, conn, repoRoot)
	stateDir := createAccessibleStateDir(t, repoRoot)

	group := &InstanceGroup{
		URI:            defaultURI,
		PoolName:       poolRef.Name,
		BaseVolumeName: testRepoRootQCOW2,
		NetworkName:    defaultNetworkName,
		StateDir:       stateDir,
		DomainPrefix:   fmt.Sprintf("provider-shutdown-%d", time.Now().UnixNano()),
		MaxSize:        2,
		VCPUCount:      2,
		MemoryMiB:      2048,
		AddressSource:  "lease",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	_, err := group.Init(ctx, hclog.NewNullLogger(), provider.Settings{
		ConnectorConfig: provider.ConnectorConfig{
			OS:       "linux",
			Arch:     "amd64",
			Protocol: provider.ProtocolSSH,
			Username: "core",
			Password: "test-password",
		},
	})
	if err != nil {
		t.Fatalf("Init() error = %v", err)
	}

	// Safety net: ensure VMs are cleaned up even if the test fails before Shutdown.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cleanupCancel()
		_ = group.Shutdown(cleanupCtx)
	})

	created, err := group.Increase(ctx, 2)
	if err != nil {
		t.Fatalf("Increase() error = %v", err)
	}
	if created != 2 {
		t.Fatalf("Increase() created = %d, want 2", created)
	}

	// Confirm that managed instances exist before calling Shutdown.
	var beforeCount int
	if err := group.Update(ctx, func(_ string, _ provider.State) {
		beforeCount++
	}); err != nil {
		t.Fatalf("Update() before Shutdown error = %v", err)
	}
	if beforeCount == 0 {
		t.Fatal("expected managed instances to exist before Shutdown")
	}

	// Shutdown must remove all VMs that carry the domain prefix — without a
	// prior Decrease call.
	if err := group.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	// After Shutdown, Update must report no remaining managed instances.
	var afterCount int
	if err := group.Update(ctx, func(_ string, _ provider.State) {
		afterCount++
	}); err != nil {
		t.Fatalf("Update() after Shutdown error = %v", err)
	}
	if afterCount != 0 {
		t.Fatalf("Shutdown() left %d managed instance(s) behind, want 0", afterCount)
	}
}

func TestLoadCACertificateIgnitionFilesFromDirectory(t *testing.T) {
	dir := t.TempDir()

	firstCert := generateSelfSignedCACertificatePEM(t, "Provider Test Root CA One")
	secondCert := generateSelfSignedCACertificatePEM(t, "Provider Test Root CA Two")

	firstPath := filepath.Join(dir, "corp root.crt")
	secondPath := filepath.Join(dir, "corp_root.pem")
	if err := os.WriteFile(firstPath, firstCert, 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", firstPath, err)
	}
	if err := os.WriteFile(secondPath, secondCert, 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", secondPath, err)
	}

	files, err := loadCACertificateIgnitionFiles(dir)
	if err != nil {
		t.Fatalf("loadCACertificateIgnitionFiles() error = %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("loadCACertificateIgnitionFiles() returned %d file(s), want 2", len(files))
	}

	got := make(map[string]string, len(files))
	for _, file := range files {
		if file.Mode == nil || *file.Mode != 0o644 {
			t.Fatalf("ignition file %q mode = %v, want 0644", file.Path, file.Mode)
		}
		if file.Overwrite == nil || !*file.Overwrite {
			t.Fatalf("ignition file %q overwrite = %v, want true", file.Path, file.Overwrite)
		}
		got[file.Path] = file.Contents.Source
	}

	want := map[string]string{
		"/etc/ssl/certs/corp-root.pem": dataURL(string(firstCert)),
		"/etc/ssl/certs/corp_root.pem": dataURL(string(secondCert)),
	}
	if len(got) != len(want) {
		t.Fatalf("ignition file map size = %d, want %d", len(got), len(want))
	}
	for path, expectedSource := range want {
		if got[path] != expectedSource {
			t.Fatalf("ignition file %q source = %q, want %q", path, got[path], expectedSource)
		}
	}
}

func TestLoadCACertificateIgnitionFilesRejectsInvalidPEM(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-cert.pem")
	if err := os.WriteFile(path, []byte("definitely not a certificate"), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}

	_, err := loadCACertificateIgnitionFiles(path)
	if err == nil {
		t.Fatal("loadCACertificateIgnitionFiles() error = nil, want PEM validation failure")
	}
	if !strings.Contains(err.Error(), "valid PEM-encoded certificates") {
		t.Fatalf("loadCACertificateIgnitionFiles() error = %v, want PEM validation detail", err)
	}
}

func TestRenderIgnitionIncludesCustomCARefreshUnit(t *testing.T) {
	cert := generateSelfSignedCACertificatePEM(t, "Provider Test Root CA")

	group := &InstanceGroup{
		settings: provider.Settings{
			ConnectorConfig: provider.ConnectorConfig{
				Username: "core",
			},
		},
		passwordHash:   "hashed-password",
		authorizedKeys: []string{"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBJfexample runner@test"},
		caCertificateFiles: []ignitionFile{
			{
				Path:      "/etc/ssl/certs/provider-test-root.pem",
				Mode:      intPtr(0o644),
				Overwrite: boolPtr(true),
				Contents: ignitionFileContents{
					Source: dataURL(string(cert)),
				},
			},
		},
	}

	rendered, err := group.renderIgnition("provider-test-host")
	if err != nil {
		t.Fatalf("renderIgnition() error = %v", err)
	}

	var cfg ignitionConfig
	if err := json.Unmarshal(rendered, &cfg); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if len(cfg.Storage.Files) != 2 {
		t.Fatalf("renderIgnition() wrote %d file(s), want 2", len(cfg.Storage.Files))
	}

	foundCustomCA := false
	for _, file := range cfg.Storage.Files {
		if file.Path == "/etc/ssl/certs/provider-test-root.pem" {
			foundCustomCA = true
			if file.Contents.Source != dataURL(string(cert)) {
				t.Fatalf("custom CA source = %q, want %q", file.Contents.Source, dataURL(string(cert)))
			}
		}
	}
	if !foundCustomCA {
		t.Fatal("renderIgnition() did not include the custom CA certificate file")
	}

	foundDocker := false
	foundCAUnit := false
	for _, unit := range cfg.Systemd.Units {
		switch unit.Name {
		case "docker.service":
			foundDocker = true
		case customCAUpdateUnitName:
			foundCAUnit = true
			if !strings.Contains(unit.Contents, "Before=docker.service") {
				t.Fatalf("custom CA unit contents missing docker ordering:\n%s", unit.Contents)
			}
			if !strings.Contains(unit.Contents, "update-ca-certificates") {
				t.Fatalf("custom CA unit contents missing trust refresh command:\n%s", unit.Contents)
			}
		}
	}
	if !foundDocker {
		t.Fatal("renderIgnition() did not keep docker.service enabled")
	}
	if !foundCAUnit {
		t.Fatal("renderIgnition() did not include the custom CA refresh unit")
	}
}

func requireSystemLibvirt(t *testing.T) *libvirt.Connect {
	t.Helper()

	conn, err := libvirt.NewConnect(defaultURI)
	if err != nil {
		t.Skipf("skipping because libvirt %q is unavailable: %v", defaultURI, err)
	}

	return conn
}

func requireDefaultNetwork(t *testing.T, conn *libvirt.Connect) {
	t.Helper()

	network, err := conn.LookupNetworkByName(defaultNetworkName)
	if err != nil {
		t.Skipf("skipping because libvirt network %q is unavailable: %v", defaultNetworkName, err)
	}
	_ = network.Free()
}

func repoRootDir(t *testing.T) string {
	t.Helper()

	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	return root
}

func requireRepoRootQCOW2(t *testing.T, repoRoot string) string {
	t.Helper()

	path := filepath.Join(repoRoot, testRepoRootQCOW2)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			t.Skipf("skipping because %q is not present in the repo root", testRepoRootQCOW2)
		}
		t.Fatalf("Stat(%q) error = %v", path, err)
	}
	return path
}

func createRepoRootPool(t *testing.T, conn *libvirt.Connect, repoRoot string) repoRootPool {
	t.Helper()

	poolDir, err := os.MkdirTemp(repoRoot, "provider-pool-dir-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	if err := os.Chmod(poolDir, 0o755); err != nil {
		t.Fatalf("Chmod(%q) error = %v", poolDir, err)
	}

	baseImagePath := requireRepoRootQCOW2(t, repoRoot)
	poolImagePath := filepath.Join(poolDir, testRepoRootQCOW2)
	if err := os.Link(baseImagePath, poolImagePath); err != nil {
		t.Fatalf("Link(%q, %q) error = %v", baseImagePath, poolImagePath, err)
	}

	name := fmt.Sprintf("provider-test-pool-%d", time.Now().UnixNano())
	xml := fmt.Sprintf(
		"<pool type='dir'><name>%s</name><target><path>%s</path></target></pool>",
		escapeXML(name),
		escapeXML(poolDir),
	)

	pool, err := conn.StoragePoolDefineXML(xml, 0)
	if err != nil {
		t.Fatalf("StoragePoolDefineXML() error = %v", err)
	}

	t.Cleanup(func() {
		_ = pool.Destroy()
		_ = pool.Undefine()
		_ = pool.Free()
		_ = os.RemoveAll(poolDir)
	})

	if err := pool.Create(0); err != nil {
		t.Fatalf("pool.Create() error = %v", err)
	}
	if err := pool.Refresh(0); err != nil {
		t.Fatalf("pool.Refresh() error = %v", err)
	}

	return repoRootPool{
		Name:          name,
		Dir:           poolDir,
		BaseImagePath: poolImagePath,
	}
}

func createAccessibleStateDir(t *testing.T, repoRoot string) string {
	t.Helper()

	dir, err := os.MkdirTemp(repoRoot, "provider-state-")
	if err != nil {
		t.Fatalf("MkdirTemp() error = %v", err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("Chmod(%q) error = %v", dir, err)
	}

	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})

	return dir
}

func generateSSHPrivateKeyPEM(t *testing.T) []byte {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey() error = %v", err)
	}

	return pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
}

func generateSelfSignedCACertificatePEM(t *testing.T, commonName string) []byte {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey() error = %v", err)
	}

	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		t.Fatalf("rand.Int() error = %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"gitlab-runner-virt-plugin"},
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("x509.CreateCertificate() error = %v", err)
	}

	return pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: der,
	})
}

func waitForManagedInstanceState(ctx context.Context, group *InstanceGroup, want provider.State) (string, error) {
	for {
		var (
			seenID    string
			seenState provider.State
		)
		err := group.Update(ctx, func(id string, state provider.State) {
			if group.managesDomain(id) {
				seenID = id
				seenState = state
			}
		})
		if err != nil {
			return "", err
		}
		if seenID != "" && seenState == want {
			return seenID, nil
		}

		if err := ctx.Err(); err != nil {
			if seenID != "" {
				return "", fmt.Errorf("timed out waiting for instance %q to reach state %q, last state %q: %w", seenID, want, seenState, err)
			}
			return "", fmt.Errorf("timed out waiting for a managed instance to reach state %q: %w", want, err)
		}

		time.Sleep(2 * time.Second)
	}
}

func waitForConnectInfo(ctx context.Context, group *InstanceGroup, instanceID string) (provider.ConnectInfo, error) {
	for {
		info, err := group.ConnectInfo(ctx, instanceID)
		if err == nil && info.ExternalAddr != "" {
			return info, nil
		}

		if ctx.Err() != nil {
			if err != nil {
				return provider.ConnectInfo{}, fmt.Errorf("timed out waiting for connect info for %q: %w", instanceID, err)
			}
			return provider.ConnectInfo{}, fmt.Errorf("timed out waiting for connect info for %q: %w", instanceID, ctx.Err())
		}

		time.Sleep(2 * time.Second)
	}
}

func waitForHeartbeat(ctx context.Context, group *InstanceGroup, instanceID string) error {
	for {
		err := group.Heartbeat(ctx, instanceID)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return fmt.Errorf("timed out waiting for heartbeat success for %q: %w", instanceID, err)
		}

		time.Sleep(2 * time.Second)
	}
}

func waitForSSHCommand(ctx context.Context, info provider.ConnectInfo, privateKeyPEM []byte, command string) (string, error) {
	var lastOutput string
	var lastErr error

	for {
		lastOutput, lastErr = runSSHCommand(info, privateKeyPEM, command)
		if lastErr == nil {
			return lastOutput, nil
		}

		if ctx.Err() != nil {
			return lastOutput, fmt.Errorf("timed out waiting for SSH command %q to succeed: %w", command, lastErr)
		}

		time.Sleep(3 * time.Second)
	}
}

func runSSHCommand(info provider.ConnectInfo, privateKeyPEM []byte, command string) (string, error) {
	authMethods := make([]ssh.AuthMethod, 0, 2)

	if len(privateKeyPEM) > 0 {
		signer, err := ssh.ParsePrivateKey(privateKeyPEM)
		if err != nil {
			return "", fmt.Errorf("ParsePrivateKey() error = %w", err)
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}
	if info.Password != "" {
		authMethods = append(authMethods, ssh.Password(info.Password))
	}

	config := &ssh.ClientConfig{
		User:            info.Username,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

	client, err := ssh.Dial("tcp", netJoinHostPort(info.ExternalAddr, info.ProtocolPort), config)
	if err != nil {
		return "", err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()

	var stdout, stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr

	err = session.Run(command)
	output := strings.TrimSpace(stdout.String())
	if stderr.Len() > 0 {
		if output != "" {
			output += "\n"
		}
		output += strings.TrimSpace(stderr.String())
	}

	if err != nil {
		return output, err
	}
	return output, nil
}

func waitForInstanceRemoved(ctx context.Context, group *InstanceGroup, instanceID string) error {
	for {
		found := false
		err := group.Update(ctx, func(id string, _ provider.State) {
			if id == instanceID {
				found = true
			}
		})
		if err != nil {
			return err
		}
		if !found {
			return nil
		}

		if err := ctx.Err(); err != nil {
			return fmt.Errorf("timed out waiting for instance %q to be removed: %w", instanceID, err)
		}

		time.Sleep(2 * time.Second)
	}
}

func netJoinHostPort(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}
