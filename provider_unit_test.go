package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"libvirt.org/go/libvirt"
)

const (
	testDriverURI        = "test:///default"
	testDriverPoolName   = "default-pool"
	testDriverDomainName = "test"
	testDriverDomainAddr = "192.168.122.3"
)

func requireTestDriverConnection(t *testing.T) *libvirt.Connect {
	t.Helper()

	conn, err := libvirt.NewConnect(testDriverURI)
	if err != nil {
		t.Skipf("skipping because libvirt test driver %q is unavailable: %v", testDriverURI, err)
	}
	return conn
}

func TestInstanceGroupApplyDefaults(t *testing.T) {
	group := &InstanceGroup{
		PoolName:       "pool",
		BaseVolumePath: "/images/base.qcow2",
		MaxSize:        1,
		DomainPrefix:   " Runner !! 2026 ",
	}

	group.applyDefaults()

	if group.URI != defaultURI {
		t.Fatalf("URI = %q, want %q", group.URI, defaultURI)
	}
	if group.NetworkName != defaultNetworkName {
		t.Fatalf("NetworkName = %q, want %q", group.NetworkName, defaultNetworkName)
	}
	if group.DomainPrefix != "runner-2026" {
		t.Fatalf("DomainPrefix = %q, want %q", group.DomainPrefix, "runner-2026")
	}
	if group.StateDir != defaultStateDir {
		t.Fatalf("StateDir = %q, want %q", group.StateDir, defaultStateDir)
	}
	if group.DomainType != defaultDomainType {
		t.Fatalf("DomainType = %q, want %q", group.DomainType, defaultDomainType)
	}
	if group.AddressSource != defaultAddressSource {
		t.Fatalf("AddressSource = %q, want %q", group.AddressSource, defaultAddressSource)
	}
	if group.VCPUCount != defaultVCPUCount {
		t.Fatalf("VCPUCount = %d, want %d", group.VCPUCount, defaultVCPUCount)
	}
	if group.MemoryMiB != defaultMemoryMiB {
		t.Fatalf("MemoryMiB = %d, want %d", group.MemoryMiB, defaultMemoryMiB)
	}
}

func TestInstanceGroupValidateConfig(t *testing.T) {
	valid := InstanceGroup{
		PoolName:       "pool",
		BaseVolumePath: "/images/base.qcow2",
		StateDir:       t.TempDir(),
		MaxSize:        1,
		AddressSource:  defaultAddressSource,
	}

	if err := valid.validateConfig(); err != nil {
		t.Fatalf("validateConfig() error = %v", err)
	}

	tests := []struct {
		name  string
		group InstanceGroup
		want  string
	}{
		{
			name: "max size",
			group: InstanceGroup{
				PoolName:       "pool",
				BaseVolumePath: "/images/base.qcow2",
				StateDir:       t.TempDir(),
				AddressSource:  defaultAddressSource,
			},
			want: "max_size must be greater than zero",
		},
		{
			name: "pool",
			group: InstanceGroup{
				BaseVolumePath: "/images/base.qcow2",
				StateDir:       t.TempDir(),
				MaxSize:        1,
				AddressSource:  defaultAddressSource,
			},
			want: "pool_name must be set",
		},
		{
			name: "base volume",
			group: InstanceGroup{
				PoolName:      "pool",
				StateDir:      t.TempDir(),
				MaxSize:       1,
				AddressSource: defaultAddressSource,
			},
			want: "either base_volume_name or base_volume_path must be set",
		},
		{
			name: "state dir",
			group: InstanceGroup{
				PoolName:       "pool",
				BaseVolumePath: "/images/base.qcow2",
				MaxSize:        1,
				AddressSource:  defaultAddressSource,
			},
			want: "state_dir must be set",
		},
		{
			name: "address source",
			group: InstanceGroup{
				PoolName:       "pool",
				BaseVolumePath: "/images/base.qcow2",
				StateDir:       t.TempDir(),
				MaxSize:        1,
				AddressSource:  "bogus",
			},
			want: "address_source must be one of auto, lease, agent, arp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.group.validateConfig()
			if err == nil {
				t.Fatalf("validateConfig() error = nil, want %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validateConfig() error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestNormalizedConnectorConfig(t *testing.T) {
	group := &InstanceGroup{}

	defaults := group.normalizedConnectorConfig(provider.ConnectorConfig{})
	if defaults.Protocol != provider.ProtocolSSH {
		t.Fatalf("Protocol = %q, want %q", defaults.Protocol, provider.ProtocolSSH)
	}
	if defaults.ProtocolPort != defaultProtocolPort {
		t.Fatalf("ProtocolPort = %d, want %d", defaults.ProtocolPort, defaultProtocolPort)
	}
	if defaults.OS != "linux" {
		t.Fatalf("OS = %q, want %q", defaults.OS, "linux")
	}
	if defaults.Arch != "amd64" {
		t.Fatalf("Arch = %q, want %q", defaults.Arch, "amd64")
	}
	if !defaults.UseStaticCredentials {
		t.Fatal("UseStaticCredentials = false, want true")
	}

	explicit := group.normalizedConnectorConfig(provider.ConnectorConfig{
		Protocol:     provider.ProtocolWinRM,
		ProtocolPort: 5985,
		OS:           "windows",
		Arch:         "arm64",
	})
	if explicit.Protocol != provider.ProtocolWinRM {
		t.Fatalf("Protocol = %q, want %q", explicit.Protocol, provider.ProtocolWinRM)
	}
	if explicit.ProtocolPort != 5985 {
		t.Fatalf("ProtocolPort = %d, want %d", explicit.ProtocolPort, 5985)
	}
	if explicit.OS != "windows" {
		t.Fatalf("OS = %q, want %q", explicit.OS, "windows")
	}
	if explicit.Arch != "arm64" {
		t.Fatalf("Arch = %q, want %q", explicit.Arch, "arm64")
	}
	if !explicit.UseStaticCredentials {
		t.Fatal("UseStaticCredentials = false, want true")
	}
}

func TestInstanceGroupInitRejectsInvalidSettings(t *testing.T) {
	baseGroup := InstanceGroup{
		PoolName:       "pool",
		BaseVolumePath: "/images/base.qcow2",
		StateDir:       t.TempDir(),
		MaxSize:        1,
		AddressSource:  defaultAddressSource,
	}

	stateDirFile := filepath.Join(t.TempDir(), "state-dir-file")
	if err := os.WriteFile(stateDirFile, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", stateDirFile, err)
	}

	tests := []struct {
		name     string
		group    InstanceGroup
		settings provider.Settings
		want     string
	}{
		{
			name:  "unsupported protocol",
			group: baseGroup,
			settings: provider.Settings{
				ConnectorConfig: provider.ConnectorConfig{
					Protocol: provider.ProtocolWinRM,
					Username: "core",
					Password: "secret",
				},
			},
			want: `flatcar instances only support ssh, got "winrm"`,
		},
		{
			name:  "missing username",
			group: baseGroup,
			settings: provider.Settings{
				ConnectorConfig: provider.ConnectorConfig{
					Protocol: provider.ProtocolSSH,
					Password: "secret",
				},
			},
			want: "connector_config.username must be set",
		},
		{
			name:  "missing credentials",
			group: baseGroup,
			settings: provider.Settings{
				ConnectorConfig: provider.ConnectorConfig{
					Protocol: provider.ProtocolSSH,
					Username: "core",
				},
			},
			want: "connector_config.password or connector_config.key must be set",
		},
		{
			name:  "invalid private key without password",
			group: baseGroup,
			settings: provider.Settings{
				ConnectorConfig: provider.ConnectorConfig{
					Protocol: provider.ProtocolSSH,
					Username: "core",
					Key:      []byte("definitely not a private key"),
				},
			},
			want: "deriving authorized key from connector key",
		},
		{
			name: "state dir creation failure",
			group: InstanceGroup{
				PoolName:       "pool",
				BaseVolumePath: "/images/base.qcow2",
				StateDir:       stateDirFile,
				MaxSize:        1,
				AddressSource:  defaultAddressSource,
			},
			settings: provider.Settings{
				ConnectorConfig: provider.ConnectorConfig{
					Protocol: provider.ProtocolSSH,
					Username: "core",
					Password: "secret",
				},
			},
			want: "creating state directory",
		},
		{
			name: "invalid custom CA path",
			group: InstanceGroup{
				PoolName:           "pool",
				BaseVolumePath:     "/images/base.qcow2",
				StateDir:           t.TempDir(),
				MaxSize:            1,
				AddressSource:      defaultAddressSource,
				CACertificatesPath: filepath.Join(t.TempDir(), "missing-ca.pem"),
			},
			settings: provider.Settings{
				ConnectorConfig: provider.ConnectorConfig{
					Protocol: provider.ProtocolSSH,
					Username: "core",
					Password: "secret",
				},
			},
			want: "loading ca certificates",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.group.Init(context.Background(), hclog.NewNullLogger(), tt.settings)
			if err == nil {
				t.Fatalf("Init() error = nil, want substring %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Init() error = %v, want substring %q", err, tt.want)
			}
		})
	}
}

func TestInstanceGroupInitWithTestDriverMissingBaseVolume(t *testing.T) {
	conn := requireTestDriverConnection(t)
	defer closeConnect(conn)

	privateKeyPEM := generateSSHPrivateKeyPEM(t)

	group := &InstanceGroup{
		URI:            testDriverURI,
		PoolName:       testDriverPoolName,
		BaseVolumeName: "missing-base.qcow2",
		NetworkName:    defaultNetworkName,
		StateDir:       t.TempDir(),
		DomainPrefix:   " Driver Test ",
		MaxSize:        1,
	}

	_, err := group.Init(context.Background(), hclog.NewNullLogger(), provider.Settings{
		ConnectorConfig: provider.ConnectorConfig{
			Protocol: provider.ProtocolSSH,
			Username: "core",
			Password: "secret",
			Key:      privateKeyPEM,
		},
	})
	if err == nil {
		t.Fatal("Init() error = nil, want base volume lookup failure")
	}
	if !strings.Contains(err.Error(), "looking up base flatcar volume") {
		t.Fatalf("Init() error = %v, want base volume lookup detail", err)
	}
	if group.passwordHash == "" {
		t.Fatal("Init() left passwordHash empty")
	}
	if len(group.authorizedKeys) != 1 {
		t.Fatalf("authorizedKeys length = %d, want 1", len(group.authorizedKeys))
	}
	if group.DomainPrefix != "driver-test" {
		t.Fatalf("DomainPrefix = %q, want %q", group.DomainPrefix, "driver-test")
	}
	if group.settings.ProtocolPort != defaultProtocolPort {
		t.Fatalf("ProtocolPort = %d, want %d", group.settings.ProtocolPort, defaultProtocolPort)
	}
	if group.settings.OS != "linux" {
		t.Fatalf("OS = %q, want %q", group.settings.OS, "linux")
	}
	if group.settings.Arch != "amd64" {
		t.Fatalf("Arch = %q, want %q", group.settings.Arch, "amd64")
	}
	if group.deleting == nil {
		t.Fatal("Init() did not initialize deleting map")
	}
}

func TestInstanceGroupUpdatePrunesDeletingWithTestDriver(t *testing.T) {
	conn := requireTestDriverConnection(t)
	defer closeConnect(conn)

	group := &InstanceGroup{
		URI:          testDriverURI,
		DomainPrefix: "managed",
		deleting: map[string]time.Time{
			"stale-instance": time.Now(),
		},
	}

	var seen []string
	if err := group.Update(context.Background(), func(id string, _ provider.State) {
		seen = append(seen, id)
	}); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	if len(seen) != 0 {
		t.Fatalf("Update() reported managed instances %v, want none", seen)
	}
	if len(group.deleting) != 0 {
		t.Fatalf("deleting map = %v, want it pruned to empty", group.deleting)
	}
}

func TestInstanceGroupShutdownWithTestDriver(t *testing.T) {
	conn := requireTestDriverConnection(t)
	defer closeConnect(conn)

	group := &InstanceGroup{
		URI:          testDriverURI,
		PoolName:     testDriverPoolName,
		DomainPrefix: "managed",
	}

	if err := group.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}

func TestInstanceGroupConnectInfoWithTestDriver(t *testing.T) {
	conn := requireTestDriverConnection(t)
	defer closeConnect(conn)

	group := &InstanceGroup{
		URI:           testDriverURI,
		AddressSource: "auto",
		settings: provider.Settings{
			ConnectorConfig: provider.ConnectorConfig{
				Protocol:     provider.ProtocolSSH,
				ProtocolPort: defaultProtocolPort,
				Username:     "core",
			},
		},
	}

	info, err := group.ConnectInfo(context.Background(), testDriverDomainName)
	if err != nil {
		t.Fatalf("ConnectInfo() error = %v", err)
	}
	if info.ID != testDriverDomainName {
		t.Fatalf("ID = %q, want %q", info.ID, testDriverDomainName)
	}
	if info.ExternalAddr != testDriverDomainAddr {
		t.Fatalf("ExternalAddr = %q, want %q", info.ExternalAddr, testDriverDomainAddr)
	}
	if info.InternalAddr != testDriverDomainAddr {
		t.Fatalf("InternalAddr = %q, want %q", info.InternalAddr, testDriverDomainAddr)
	}
	if info.Username != "core" {
		t.Fatalf("Username = %q, want %q", info.Username, "core")
	}
	if info.ProtocolPort != defaultProtocolPort {
		t.Fatalf("ProtocolPort = %d, want %d", info.ProtocolPort, defaultProtocolPort)
	}
}

func TestInstanceGroupAddressDiscoveryWithTestDriver(t *testing.T) {
	conn := requireTestDriverConnection(t)
	defer closeConnect(conn)

	dom, err := conn.LookupDomainByName(testDriverDomainName)
	if err != nil {
		t.Fatalf("LookupDomainByName(%q) error = %v", testDriverDomainName, err)
	}
	defer dom.Free()

	for _, source := range []string{"lease", "agent", "arp", "auto"} {
		t.Run(source, func(t *testing.T) {
			group := &InstanceGroup{
				NetworkName:        defaultNetworkName,
				AddressSource:      source,
				settings:           provider.Settings{},
				caCertificateFiles: nil,
			}

			addr, err := group.discoverAddress(context.Background(), conn, dom)
			if err != nil {
				t.Fatalf("discoverAddress(%q) error = %v", source, err)
			}
			if addr != testDriverDomainAddr {
				t.Fatalf("discoverAddress(%q) = %q, want %q", source, addr, testDriverDomainAddr)
			}
		})
	}

	macs, err := domainMACAddresses(dom)
	if err != nil {
		t.Fatalf("domainMACAddresses() error = %v", err)
	}
	if len(macs) != 1 || macs[0] != "aa:bb:cc:dd:ee:ff" {
		t.Fatalf("domainMACAddresses() = %v, want [aa:bb:cc:dd:ee:ff]", macs)
	}

	_, err = (&InstanceGroup{NetworkName: defaultNetworkName}).addressFromNetworkLeases(context.Background(), conn, dom)
	if err == nil {
		t.Fatal("addressFromNetworkLeases() error = nil, want unsupported lease lookup")
	}
	if !strings.Contains(err.Error(), "reading dhcp leases") {
		t.Fatalf("addressFromNetworkLeases() error = %v, want DHCP lease detail", err)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = (&InstanceGroup{}).addressFromDomainSource(canceledCtx, dom, libvirt.DOMAIN_INTERFACE_ADDRESSES_SRC_LEASE)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("addressFromDomainSource() error = %v, want %v", err, context.Canceled)
	}

	_, err = (&InstanceGroup{NetworkName: defaultNetworkName}).addressFromNetworkLeases(canceledCtx, conn, dom)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("addressFromNetworkLeases() error = %v, want %v", err, context.Canceled)
	}

	_, err = (&InstanceGroup{AddressSource: "auto"}).waitForAddress(canceledCtx, conn, dom, time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForAddress() error = %v, want %v", err, context.Canceled)
	}
}

func TestInstanceGroupCurrentStateWithTestDriver(t *testing.T) {
	conn := requireTestDriverConnection(t)
	defer closeConnect(conn)

	running, err := conn.LookupDomainByName(testDriverDomainName)
	if err != nil {
		t.Fatalf("LookupDomainByName(%q) error = %v", testDriverDomainName, err)
	}
	defer running.Free()

	group := &InstanceGroup{
		NetworkName:   defaultNetworkName,
		AddressSource: "auto",
	}

	state, err := group.currentState(context.Background(), conn, running, false)
	if err != nil {
		t.Fatalf("currentState(running) error = %v", err)
	}
	if state != provider.StateRunning {
		t.Fatalf("currentState(running) = %q, want %q", state, provider.StateRunning)
	}

	state, err = group.currentState(context.Background(), conn, running, true)
	if err != nil {
		t.Fatalf("currentState(deleting) error = %v", err)
	}
	if state != provider.StateDeleting {
		t.Fatalf("currentState(deleting) = %q, want %q", state, provider.StateDeleting)
	}

	id := fmt.Sprintf("state-test-%d", time.Now().UnixNano())
	stopped, err := conn.DomainDefineXML(fmt.Sprintf(
		"<domain type='test'><name>%s</name><memory unit='KiB'>8192</memory><os><type>hvm</type></os></domain>",
		id,
	))
	if err != nil {
		t.Fatalf("DomainDefineXML() error = %v", err)
	}
	defer func() {
		_ = stopped.Undefine()
		_ = stopped.Free()
	}()

	state, err = group.currentState(context.Background(), conn, stopped, false)
	if err != nil {
		t.Fatalf("currentState(stopped) error = %v", err)
	}
	if state != provider.StateTimeout {
		t.Fatalf("currentState(stopped) = %q, want %q", state, provider.StateTimeout)
	}

	_, err = group.addressFromDomainSource(context.Background(), stopped, libvirt.DOMAIN_INTERFACE_ADDRESSES_SRC_LEASE)
	if err == nil {
		t.Fatal("addressFromDomainSource() error = nil, want stopped-domain lookup failure")
	}
}

func TestInstanceGroupDeleteInstanceWithTestDriver(t *testing.T) {
	conn := requireTestDriverConnection(t)
	defer closeConnect(conn)

	pool, err := conn.LookupStoragePoolByName(testDriverPoolName)
	if err != nil {
		t.Fatalf("LookupStoragePoolByName(%q) error = %v", testDriverPoolName, err)
	}
	defer pool.Free()

	id := fmt.Sprintf("cleanup-%d", time.Now().UnixNano())
	domain, err := conn.DomainDefineXML(fmt.Sprintf(
		"<domain type='test'><name>%s</name><memory unit='KiB'>8192</memory><os><type>hvm</type></os></domain>",
		id,
	))
	if err != nil {
		t.Fatalf("DomainDefineXML() error = %v", err)
	}
	_ = domain.Free()

	volume, err := pool.StorageVolCreateXML(
		fmt.Sprintf(
			"<volume><name>%s</name><capacity unit='bytes'>1024</capacity><target><format type='qcow2'/></target></volume>",
			escapeXML(id+".img"),
		),
		0,
	)
	if err != nil {
		t.Fatalf("StorageVolCreateXML() error = %v", err)
	}
	_ = volume.Free()

	group := &InstanceGroup{StateDir: t.TempDir()}
	ignitionPath := group.ignitionPath(id)
	if err := os.MkdirAll(filepath.Dir(ignitionPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(ignitionPath), err)
	}
	if err := os.WriteFile(ignitionPath, []byte("ignition"), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", ignitionPath, err)
	}

	if err := group.deleteInstance(conn, pool, id); err != nil {
		t.Fatalf("deleteInstance() error = %v", err)
	}

	_, err = conn.LookupDomainByName(id)
	if !errors.Is(err, libvirt.ERR_NO_DOMAIN) {
		t.Fatalf("LookupDomainByName(%q) error = %v, want %v", id, err, libvirt.ERR_NO_DOMAIN)
	}

	_, err = pool.LookupStorageVolByName(group.volumeName(id))
	if !errors.Is(err, libvirt.ERR_NO_STORAGE_VOL) {
		t.Fatalf("LookupStorageVolByName(%q) error = %v, want %v", group.volumeName(id), err, libvirt.ERR_NO_STORAGE_VOL)
	}

	if _, err := os.Stat(ignitionPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Stat(%q) error = %v, want %v", ignitionPath, err, os.ErrNotExist)
	}
}

func TestInstanceGroupCreateInstanceCleansUpWhenResizeFailsWithTestDriver(t *testing.T) {
	conn := requireTestDriverConnection(t)
	defer closeConnect(conn)

	pool, err := conn.LookupStoragePoolByName(testDriverPoolName)
	if err != nil {
		t.Fatalf("LookupStoragePoolByName(%q) error = %v", testDriverPoolName, err)
	}
	defer pool.Free()

	group := &InstanceGroup{
		StateDir:    t.TempDir(),
		DiskSizeGiB: 1,
		settings: provider.Settings{
			ConnectorConfig: provider.ConnectorConfig{
				Username: "core",
			},
		},
	}

	id := fmt.Sprintf("resize-fail-%d", time.Now().UnixNano())
	err = group.createInstance(conn, pool, baseVolumeDetails{
		Capacity: 1,
		Format:   "qcow2",
		Path:     "/base.qcow2",
	}, id)
	if err == nil {
		t.Fatal("createInstance() error = nil, want resize failure")
	}
	if !strings.Contains(err.Error(), "resizing cloned volume") {
		t.Fatalf("createInstance() error = %v, want resize failure detail", err)
	}

	if err := pool.Refresh(0); err != nil {
		t.Fatalf("pool.Refresh() error = %v", err)
	}

	_, err = pool.LookupStorageVolByName(group.volumeName(id))
	if !errors.Is(err, libvirt.ERR_NO_STORAGE_VOL) {
		t.Fatalf("LookupStorageVolByName(%q) error = %v, want %v", group.volumeName(id), err, libvirt.ERR_NO_STORAGE_VOL)
	}

	if _, err := conn.LookupDomainByName(id); !errors.Is(err, libvirt.ERR_NO_DOMAIN) {
		t.Fatalf("LookupDomainByName(%q) error = %v, want %v", id, err, libvirt.ERR_NO_DOMAIN)
	}

	if _, err := os.Stat(group.ignitionPath(id)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Stat(%q) error = %v, want %v", group.ignitionPath(id), err, os.ErrNotExist)
	}
}

func TestInstanceGroupHeartbeatReturnsUnhealthyForMissingDomain(t *testing.T) {
	conn := requireTestDriverConnection(t)
	defer closeConnect(conn)

	group := &InstanceGroup{
		URI: testDriverURI,
	}

	err := group.Heartbeat(context.Background(), "missing-domain")
	if !errors.Is(err, provider.ErrInstanceUnhealthy) {
		t.Fatalf("Heartbeat() error = %v, want %v", err, provider.ErrInstanceUnhealthy)
	}
}

func TestInstanceGroupIncreaseDecreaseShortCircuit(t *testing.T) {
	group := &InstanceGroup{}

	created, err := group.Increase(context.Background(), 0)
	if err != nil {
		t.Fatalf("Increase(0) error = %v", err)
	}
	if created != 0 {
		t.Fatalf("Increase(0) = %d, want 0", created)
	}

	removed, err := group.Decrease(context.Background(), nil)
	if err != nil {
		t.Fatalf("Decrease(nil) error = %v", err)
	}
	if removed != nil {
		t.Fatalf("Decrease(nil) = %v, want nil", removed)
	}
}

func TestRenderIgnitionForNonCoreUser(t *testing.T) {
	group := &InstanceGroup{
		settings: provider.Settings{
			ConnectorConfig: provider.ConnectorConfig{
				Username: "runner",
			},
		},
		passwordHash:   "hashed-password",
		authorizedKeys: []string{"ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCu example@test"},
	}

	rendered, err := group.renderIgnition("runner-host")
	if err != nil {
		t.Fatalf("renderIgnition() error = %v", err)
	}

	var cfg ignitionConfig
	if err := json.Unmarshal(rendered, &cfg); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if len(cfg.Passwd.Users) != 1 {
		t.Fatalf("renderIgnition() wrote %d user(s), want 1", len(cfg.Passwd.Users))
	}
	user := cfg.Passwd.Users[0]
	if user.Name != "runner" {
		t.Fatalf("user.Name = %q, want %q", user.Name, "runner")
	}
	if len(user.Groups) != 2 || user.Groups[0] != "sudo" || user.Groups[1] != "docker" {
		t.Fatalf("user.Groups = %v, want [sudo docker]", user.Groups)
	}
	if len(cfg.Storage.Files) != 1 || cfg.Storage.Files[0].Path != "/etc/hostname" {
		t.Fatalf("cfg.Storage.Files = %+v, want only /etc/hostname", cfg.Storage.Files)
	}
	if len(cfg.Systemd.Units) != 1 || cfg.Systemd.Units[0].Name != "docker.service" {
		t.Fatalf("cfg.Systemd.Units = %+v, want only docker.service", cfg.Systemd.Units)
	}
}

func TestInstanceGroupSmallHelpers(t *testing.T) {
	group := &InstanceGroup{
		DomainPrefix:   "runner",
		StateDir:       "/var/tmp/provider",
		BaseVolumeName: "base.qcow2",
		DiskSizeGiB:    10,
	}

	if got := group.diskSizeBytes(); got != 10*1024*1024*1024 {
		t.Fatalf("diskSizeBytes() = %d, want %d", got, 10*1024*1024*1024)
	}
	if got := group.baseVolumeLabel(); got != "base.qcow2" {
		t.Fatalf("baseVolumeLabel() = %q, want %q", got, "base.qcow2")
	}
	if !group.managesDomain("runner-123") {
		t.Fatal("managesDomain() = false, want true")
	}
	if group.managesDomain("other-123") {
		t.Fatal("managesDomain() = true, want false")
	}
	if got := group.volumeName("runner-123"); got != "runner-123.img" {
		t.Fatalf("volumeName() = %q, want %q", got, "runner-123.img")
	}
	if got := group.ignitionPath("runner-123"); got != filepath.Join(group.StateDir, "runner-123.ign") {
		t.Fatalf("ignitionPath() = %q, want %q", got, filepath.Join(group.StateDir, "runner-123.ign"))
	}

	name, err := group.nextInstanceName()
	if err != nil {
		t.Fatalf("nextInstanceName() error = %v", err)
	}
	if matched, err := regexp.MatchString(`^runner-\d{8}-\d{6}-[0-9a-f]{8}$`, name); err != nil {
		t.Fatalf("regexp.MatchString() error = %v", err)
	} else if !matched {
		t.Fatalf("nextInstanceName() = %q, want runner timestamp/random suffix", name)
	}

	group.markDeleting("runner-123")
	snapshot := group.snapshotDeleting()
	if !snapshot["runner-123"] {
		t.Fatalf("snapshotDeleting() = %v, want runner-123 present", snapshot)
	}

	group.pruneDeleting(map[string]struct{}{"runner-123": {}})
	if len(group.deleting) != 1 {
		t.Fatalf("deleting map length = %d, want 1", len(group.deleting))
	}

	group.unmarkDeleting("runner-123")
	if len(group.deleting) != 0 {
		t.Fatalf("deleting map = %v, want empty", group.deleting)
	}
}

func TestXMLAndAddressHelpers(t *testing.T) {
	rendered, err := renderDomainXML(domainTemplateData{
		DomainType:    "test",
		Name:          "vm<&>",
		Description:   "managed<&>",
		MemoryMiB:     2048,
		VCPUCount:     2,
		Arch:          "x86_64",
		MachineType:   "pc-q35-8.2",
		DiskFormat:    "qcow2",
		DiskPath:      "/tmp/disk<&>.img",
		MACAddress:    "52:54:00:aa:bb:cc",
		NetworkName:   "default",
		FWCfgArgument: "name=opt/org.flatcar-linux/config,file=/tmp/test<&>.ign",
	})
	if err != nil {
		t.Fatalf("renderDomainXML() error = %v", err)
	}
	if !strings.Contains(rendered, "<name>vm&lt;&amp;&gt;</name>") {
		t.Fatalf("renderDomainXML() missing escaped name:\n%s", rendered)
	}
	if !strings.Contains(rendered, "machine='pc-q35-8.2'") {
		t.Fatalf("renderDomainXML() missing machine type:\n%s", rendered)
	}
	if !strings.Contains(rendered, "file=/tmp/test&lt;&amp;&gt;.ign") {
		t.Fatalf("renderDomainXML() missing escaped fw_cfg file:\n%s", rendered)
	}

	overlay := renderVolumeOverlayXML("overlay<&>.img", baseVolumeDetails{
		Capacity: 2048,
		Format:   "qcow2",
		Path:     "/images/base<&>.qcow2",
	})
	if got := volumeFormatFromXML(overlay); got != "qcow2" {
		t.Fatalf("volumeFormatFromXML() = %q, want %q", got, "qcow2")
	}
	if got := volumeBackingStoreFormatFromXML(overlay); got != "qcow2" {
		t.Fatalf("volumeBackingStoreFormatFromXML() = %q, want %q", got, "qcow2")
	}
	if got := volumeBackingStorePathFromXML(overlay); got != "/images/base<&>.qcow2" {
		t.Fatalf("volumeBackingStorePathFromXML() = %q, want %q", got, "/images/base<&>.qcow2")
	}

	rawOverlay := renderVolumeOverlayXML("overlay-raw.img", baseVolumeDetails{
		Capacity: 2048,
		Format:   "raw",
		Path:     "/images/base.img",
	})
	if got := volumeFormatFromXML(rawOverlay); got != "qcow2" {
		t.Fatalf("volumeFormatFromXML(raw backing) = %q, want %q (overlay is always qcow2)", got, "qcow2")
	}
	if got := volumeBackingStoreFormatFromXML(rawOverlay); got != "raw" {
		t.Fatalf("volumeBackingStoreFormatFromXML(raw backing) = %q, want %q", got, "raw")
	}

	emptyFormatOverlay := renderVolumeOverlayXML("overlay-default.img", baseVolumeDetails{
		Capacity: 1024,
		Path:     "/images/base.qcow2",
	})
	if got := volumeBackingStoreFormatFromXML(emptyFormatOverlay); got != "qcow2" {
		t.Fatalf("volumeBackingStoreFormatFromXML(empty format) = %q, want %q (default)", got, "qcow2")
	}

	if got := volumeFormatFromXML("definitely-not-xml"); got != "" {
		t.Fatalf("volumeFormatFromXML(invalid) = %q, want empty string", got)
	}
	if got := volumeBackingStoreFormatFromXML("definitely-not-xml"); got != "" {
		t.Fatalf("volumeBackingStoreFormatFromXML(invalid) = %q, want empty string", got)
	}

	ifaces := []libvirt.DomainInterface{
		{
			Addrs: []libvirt.DomainIPAddress{
				{Type: libvirt.IP_ADDR_TYPE_IPV6, Addr: "fe80::1"},
				{Type: libvirt.IP_ADDR_TYPE_IPV4, Addr: "192.0.2.10"},
			},
		},
	}
	if got := selectAddressFromInterfaces(ifaces); got != "192.0.2.10" {
		t.Fatalf("selectAddressFromInterfaces() = %q, want %q", got, "192.0.2.10")
	}
	if got := selectAddressFromInterfaces([]libvirt.DomainInterface{
		{
			Addrs: []libvirt.DomainIPAddress{
				{Type: libvirt.IP_ADDR_TYPE_IPV6, Addr: "2001:db8::1"},
			},
		},
	}); got != "2001:db8::1" {
		t.Fatalf("selectAddressFromInterfaces(ipv6 fallback) = %q, want %q", got, "2001:db8::1")
	}

	normalizeTests := map[string]string{
		" 127.0.0.1 ": "",
		"169.254.1.1": "",
		"::1":         "",
		"192.0.2.5":   "192.0.2.5",
		"2001:db8::5": "2001:db8::5",
		"not-an-ip":   "",
	}
	for raw, want := range normalizeTests {
		if got := normalizeDiscoveredIP(raw); got != want {
			t.Fatalf("normalizeDiscoveredIP(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestCertificateAndFileHelpers(t *testing.T) {
	first := generateSelfSignedCACertificatePEM(t, "Provider Test CA One")
	second := generateSelfSignedCACertificatePEM(t, "Provider Test CA Two")

	if err := validatePEMCertificateBundle(append(append([]byte(nil), first...), second...)); err != nil {
		t.Fatalf("validatePEMCertificateBundle(bundle) error = %v", err)
	}

	if err := validatePEMCertificateBundle(nil); err == nil || !strings.Contains(err.Error(), "file is empty") {
		t.Fatalf("validatePEMCertificateBundle(nil) error = %v, want empty file detail", err)
	}

	privateKey := generateSSHPrivateKeyPEM(t)
	if err := validatePEMCertificateBundle(privateKey); err == nil || !strings.Contains(err.Error(), `unsupported PEM block type "RSA PRIVATE KEY"`) {
		t.Fatalf("validatePEMCertificateBundle(private key) error = %v, want unsupported block detail", err)
	}

	badCert := []byte("-----BEGIN CERTIFICATE-----\nbm90LWEtdmFsaWQtY2VydA==\n-----END CERTIFICATE-----\n")
	if err := validatePEMCertificateBundle(badCert); err == nil || !strings.Contains(err.Error(), "parsing certificate") {
		t.Fatalf("validatePEMCertificateBundle(bad cert) error = %v, want parse detail", err)
	}

	certFile := filepath.Join(t.TempDir(), "bundle.pem")
	if err := os.WriteFile(certFile, first, 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", certFile, err)
	}
	paths, err := caCertificateHostPaths(certFile)
	if err != nil {
		t.Fatalf("caCertificateHostPaths(file) error = %v", err)
	}
	if len(paths) != 1 || paths[0] != certFile {
		t.Fatalf("caCertificateHostPaths(file) = %v, want [%q]", paths, certFile)
	}

	dir := t.TempDir()
	firstPath := filepath.Join(dir, "b.crt")
	secondPath := filepath.Join(dir, "a.crt")
	if err := os.WriteFile(firstPath, first, 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", firstPath, err)
	}
	if err := os.WriteFile(secondPath, second, 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", secondPath, err)
	}
	if err := os.Mkdir(filepath.Join(dir, "nested"), 0o755); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", filepath.Join(dir, "nested"), err)
	}

	paths, err = caCertificateHostPaths(dir)
	if err != nil {
		t.Fatalf("caCertificateHostPaths(dir) error = %v", err)
	}
	wantPaths := []string{secondPath, firstPath}
	if len(paths) != len(wantPaths) || paths[0] != wantPaths[0] || paths[1] != wantPaths[1] {
		t.Fatalf("caCertificateHostPaths(dir) = %v, want %v", paths, wantPaths)
	}

	emptyDir := t.TempDir()
	if _, err := caCertificateHostPaths(emptyDir); err == nil || !strings.Contains(err.Error(), "no certificate files found") {
		t.Fatalf("caCertificateHostPaths(emptyDir) error = %v, want no-files detail", err)
	}

	if got := sanitizeCACertificateFileName(" corp root .crt ", 0); got != "corp-root.pem" {
		t.Fatalf("sanitizeCACertificateFileName() = %q, want %q", got, "corp-root.pem")
	}
	if got := sanitizeCACertificateFileName("!!!", 1); got != "custom-ca-02.pem" {
		t.Fatalf("sanitizeCACertificateFileName() = %q, want %q", got, "custom-ca-02.pem")
	}

	used := map[string]int{}
	if got := uniqueCACertificateFileName("corp-root.pem", used); got != "corp-root.pem" {
		t.Fatalf("uniqueCACertificateFileName(first) = %q, want %q", got, "corp-root.pem")
	}
	if got := uniqueCACertificateFileName("corp-root.pem", used); got != "corp-root-2.pem" {
		t.Fatalf("uniqueCACertificateFileName(second) = %q, want %q", got, "corp-root-2.pem")
	}

	path := filepath.Join(t.TempDir(), "nested", "config.ign")
	if err := writeFileAtomic(path, []byte("first"), 0o600); err != nil {
		t.Fatalf("writeFileAtomic(first) error = %v", err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	if string(content) != "first" {
		t.Fatalf("file content = %q, want %q", string(content), "first")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%q) error = %v", path, err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("file mode = %v, want %v", info.Mode().Perm(), os.FileMode(0o600))
	}

	if err := writeFileAtomic(path, []byte("second"), 0o644); err != nil {
		t.Fatalf("writeFileAtomic(second) error = %v", err)
	}
	content, err = os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	if string(content) != "second" {
		t.Fatalf("file content = %q, want %q", string(content), "second")
	}
	info, err = os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%q) error = %v", path, err)
	}
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("file mode = %v, want %v", info.Mode().Perm(), os.FileMode(0o644))
	}
}

func TestMiscHelpers(t *testing.T) {
	if got := libvirtArch(""); got != "x86_64" {
		t.Fatalf("libvirtArch(\"\") = %q, want %q", got, "x86_64")
	}
	if got := libvirtArch("arm64"); got != "aarch64" {
		t.Fatalf("libvirtArch(%q) = %q, want %q", "arm64", got, "aarch64")
	}
	if got := libvirtArch("s390x"); got != "s390x" {
		t.Fatalf("libvirtArch(%q) = %q, want %q", "s390x", got, "s390x")
	}

	if got := sanitizeDomainPrefix("  Team Runner !! "); got != "team-runner" {
		t.Fatalf("sanitizeDomainPrefix() = %q, want %q", got, "team-runner")
	}
	if got := sanitizeDomainPrefix(" !!! "); got != defaultDomainPrefix {
		t.Fatalf("sanitizeDomainPrefix() = %q, want %q", got, defaultDomainPrefix)
	}

	mac, err := newMACAddress()
	if err != nil {
		t.Fatalf("newMACAddress() error = %v", err)
	}
	if matched, err := regexp.MatchString(`^52:54:00:[0-9a-f]{2}:[0-9a-f]{2}:[0-9a-f]{2}$`, mac); err != nil {
		t.Fatalf("regexp.MatchString() error = %v", err)
	} else if !matched {
		t.Fatalf("newMACAddress() = %q, want qemu-style MAC", mac)
	}

	authorizedKeys, err := authorizedKeysFromPrivateKey(generateSSHPrivateKeyPEM(t))
	if err != nil {
		t.Fatalf("authorizedKeysFromPrivateKey() error = %v", err)
	}
	if len(authorizedKeys) != 1 || !strings.HasPrefix(authorizedKeys[0], "ssh-rsa ") {
		t.Fatalf("authorizedKeysFromPrivateKey() = %v, want one ssh-rsa key", authorizedKeys)
	}
	if _, err := authorizedKeysFromPrivateKey([]byte("not a private key")); err == nil {
		t.Fatal("authorizedKeysFromPrivateKey(invalid) error = nil, want parse failure")
	}

	if got := dataURL("hello world\n"); got != "data:,hello%20world%0A" {
		t.Fatalf("dataURL() = %q, want %q", got, "data:,hello%20world%0A")
	}
	if got := formatLibvirtVersion(10000000); got != "10.0.0" {
		t.Fatalf("formatLibvirtVersion() = %q, want %q", got, "10.0.0")
	}

	if value := boolPtr(true); value == nil || !*value {
		t.Fatalf("boolPtr(true) = %v, want pointer to true", value)
	}
	if value := intPtr(42); value == nil || *value != 42 {
		t.Fatalf("intPtr(42) = %v, want pointer to 42", value)
	}
}
