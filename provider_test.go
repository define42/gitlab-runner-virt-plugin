package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
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

const testRepoRootQCOW2 = "flatcar_production_qemu_image.qcow2"

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

func TestResolveBaseVolumeAndCloneRepoRootQCOW2(t *testing.T) {
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

	cloneName := fmt.Sprintf("provider-test-clone-%d.qcow2", time.Now().UnixNano())
	clone, err := pool.StorageVolCreateXMLFrom(renderVolumeCloneXML(cloneName, details), baseVol, 0)
	if err != nil {
		t.Fatalf("StorageVolCreateXMLFrom() error = %v", err)
	}
	defer func() {
		_ = clone.Delete(0)
		_ = clone.Free()
	}()

	desc, err := clone.GetXMLDesc(0)
	if err != nil {
		t.Fatalf("clone.GetXMLDesc() error = %v", err)
	}
	if got := volumeFormatFromXML(desc); got != "qcow2" {
		t.Fatalf("clone format = %q, want %q", got, "qcow2")
	}

	clonePath, err := clone.GetPath()
	if err != nil {
		t.Fatalf("clone.GetPath() error = %v", err)
	}
	if want := filepath.Join(poolRef.Dir, cloneName); clonePath != want {
		t.Fatalf("clone path = %q, want %q", clonePath, want)
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
