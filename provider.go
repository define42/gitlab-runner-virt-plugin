package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"encoding/xml"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/GehirnInc/crypt/sha512_crypt"
	"github.com/hashicorp/go-hclog"
	"gitlab.com/gitlab-org/fleeting/fleeting/provider"
	"golang.org/x/crypto/ssh"
	"libvirt.org/go/libvirt"
)

const (
	defaultURI               = "qemu:///system"
	defaultNetworkName       = "default"
	defaultDomainPrefix      = "gitlab-runner"
	defaultStateDir          = "/var/lib/libvirt/gitlab-runner-virt-plugin"
	defaultDomainType        = "kvm"
	defaultAddressSource     = "auto"
	defaultVCPUCount         = 2
	defaultMemoryMiB         = 4096
	defaultProtocolPort      = 22
	defaultConnectInfoWait   = 15 * time.Second
	defaultHeartbeatTimeout  = 5 * time.Second
	managedDescriptionPrefix = "managed-by:gitlab-runner-virt-plugin"
	ignitionVersion          = "3.3.0"
	flatcarIgnitionFWCfgName = "opt/org.flatcar-linux/config"
	customCAUpdateUnitName   = "gitlab-runner-update-ca-certificates.service"
)

var (
	_ provider.InstanceGroup = (*InstanceGroup)(nil)

	domainPrefixSanitizer = regexp.MustCompile(`[^a-zA-Z0-9-]+`)
	caFileNameSanitizer   = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)
	domainXMLTmpl         = template.Must(template.New("domain").Funcs(template.FuncMap{
		"xml": escapeXML,
	}).Parse(`
<domain type='{{xml .DomainType}}' xmlns:qemu='http://libvirt.org/schemas/domain/qemu/1.0'>
  <name>{{xml .Name}}</name>
  <description>{{xml .Description}}</description>
  <memory unit='MiB'>{{.MemoryMiB}}</memory>
  <currentMemory unit='MiB'>{{.MemoryMiB}}</currentMemory>
  <vcpu placement='static'>{{.VCPUCount}}</vcpu>
  <os>
    <type arch='{{xml .Arch}}'{{if .MachineType}} machine='{{xml .MachineType}}'{{end}}>hvm</type>
  </os>
  <features>
    <acpi/>
    <apic/>
  </features>
  <cpu mode='host-passthrough' check='none' migratable='on'/>
  <clock offset='utc'>
    <timer name='rtc' tickpolicy='catchup'/>
    <timer name='pit' tickpolicy='delay'/>
    <timer name='hpet' present='no'/>
  </clock>
  <pm>
    <suspend-to-mem enabled='no'/>
    <suspend-to-disk enabled='no'/>
  </pm>
  <on_poweroff>destroy</on_poweroff>
  <on_reboot>restart</on_reboot>
  <on_crash>destroy</on_crash>
  <devices>
    <disk type='file' device='disk'>
      <driver name='qemu' type='{{xml .DiskFormat}}'/>
      <source file='{{xml .DiskPath}}'/>
      <target dev='hda' bus='ide'/>
    </disk>
    <interface type='network'>
      <mac address='{{xml .MACAddress}}'/>
      <source network='{{xml .NetworkName}}'/>
      <model type='e1000'/>
    </interface>
    <serial type='pty'>
      <target port='0'/>
    </serial>
    <console type='pty'>
      <target type='serial' port='0'/>
    </console>
    <rng model='virtio'>
      <backend model='random'>/dev/urandom</backend>
    </rng>
  </devices>
  <qemu:commandline>
    <qemu:arg value='-fw_cfg'/>
    <qemu:arg value='{{xml .FWCfgArgument}}'/>
  </qemu:commandline>
</domain>`))
)

type baseVolumeDetails struct {
	Capacity uint64
	Format   string
	Path     string
}

type domainTemplateData struct {
	DomainType    string
	Name          string
	Description   string
	MemoryMiB     uint
	VCPUCount     uint
	Arch          string
	MachineType   string
	DiskFormat    string
	DiskPath      string
	MACAddress    string
	NetworkName   string
	FWCfgArgument string
}

type storageVolumeXML struct {
	Target struct {
		Format struct {
			Type string `xml:"type,attr"`
		} `xml:"format"`
	} `xml:"target"`
	BackingStore struct {
		Path   string `xml:"path"`
		Format struct {
			Type string `xml:"type,attr"`
		} `xml:"format"`
	} `xml:"backingStore"`
}

type libvirtDomainXML struct {
	Devices struct {
		Interfaces []struct {
			MAC struct {
				Address string `xml:"address,attr"`
			} `xml:"mac"`
		} `xml:"interface"`
	} `xml:"devices"`
}

type ignitionConfig struct {
	Ignition ignitionSection `json:"ignition"`
	Passwd   ignitionPasswd  `json:"passwd,omitempty"`
	Storage  ignitionStorage `json:"storage,omitempty"`
	Systemd  ignitionSystemd `json:"systemd,omitempty"`
}

type ignitionSection struct {
	Version string `json:"version"`
}

type ignitionPasswd struct {
	Users []ignitionUser `json:"users,omitempty"`
}

type ignitionUser struct {
	Name              string   `json:"name"`
	PasswordHash      string   `json:"passwordHash,omitempty"`
	SSHAuthorizedKeys []string `json:"sshAuthorizedKeys,omitempty"`
	Groups            []string `json:"groups,omitempty"`
}

type ignitionStorage struct {
	Files []ignitionFile `json:"files,omitempty"`
}

type ignitionFile struct {
	Path      string               `json:"path"`
	Mode      *int                 `json:"mode,omitempty"`
	Overwrite *bool                `json:"overwrite,omitempty"`
	Contents  ignitionFileContents `json:"contents"`
}

type ignitionFileContents struct {
	Source string `json:"source"`
}

type ignitionSystemd struct {
	Units []ignitionUnit `json:"units,omitempty"`
}

type ignitionUnit struct {
	Name     string `json:"name"`
	Enabled  *bool  `json:"enabled,omitempty"`
	Contents string `json:"contents,omitempty"`
}

func (g *InstanceGroup) Init(ctx context.Context, logger hclog.Logger, settings provider.Settings) (provider.ProviderInfo, error) {
	g.logger = logger
	g.settings = settings
	g.applyDefaults()
	if err := g.validateConfig(); err != nil {
		return provider.ProviderInfo{}, err
	}
	g.settings.ConnectorConfig = g.normalizedConnectorConfig(settings.ConnectorConfig)
	g.passwordHash = ""
	g.authorizedKeys = nil
	g.caCertificateFiles = nil

	if g.settings.Protocol != provider.ProtocolSSH {
		return provider.ProviderInfo{}, fmt.Errorf("flatcar instances only support ssh, got %q", g.settings.Protocol)
	}
	if g.settings.Username == "" {
		return provider.ProviderInfo{}, fmt.Errorf("connector_config.username must be set")
	}
	if g.settings.Password == "" && len(g.settings.Key) == 0 {
		return provider.ProviderInfo{}, fmt.Errorf("connector_config.password or connector_config.key must be set")
	}

	if err := os.MkdirAll(g.StateDir, 0o755); err != nil {
		return provider.ProviderInfo{}, fmt.Errorf("creating state directory: %w", err)
	}

	if g.settings.Password != "" {
		hashed, err := sha512_crypt.New().Generate([]byte(g.settings.Password), nil)
		if err != nil {
			return provider.ProviderInfo{}, fmt.Errorf("hashing connector password: %w", err)
		}
		g.passwordHash = hashed
	}

	if len(g.settings.Key) > 0 {
		keys, err := authorizedKeysFromPrivateKey(g.settings.Key)
		if err != nil {
			if g.settings.Password == "" {
				return provider.ProviderInfo{}, fmt.Errorf("deriving authorized key from connector key: %w", err)
			}
			g.logger.Warn("ignoring connector key for ignition because the public key could not be derived", "err", err)
		} else {
			g.authorizedKeys = keys
		}
	}

	if g.CACertificatesPath != "" {
		files, err := loadCACertificateIgnitionFiles(g.CACertificatesPath)
		if err != nil {
			return provider.ProviderInfo{}, fmt.Errorf("loading ca certificates: %w", err)
		}
		g.caCertificateFiles = files
	}

	g.mu.Lock()
	if g.deleting == nil {
		g.deleting = make(map[string]time.Time)
	}
	g.mu.Unlock()

	conn, err := g.connect()
	if err != nil {
		return provider.ProviderInfo{}, err
	}
	defer closeConnect(conn)

	version, err := conn.GetLibVersion()
	if err != nil {
		return provider.ProviderInfo{}, fmt.Errorf("querying libvirt version: %w", err)
	}

	if err := g.validateLibvirtResources(conn); err != nil {
		return provider.ProviderInfo{}, err
	}

	return provider.ProviderInfo{
		ID:        fmt.Sprintf("libvirt/%s/%s", g.URI, g.DomainPrefix),
		MaxSize:   g.MaxSize,
		Version:   formatLibvirtVersion(version),
		BuildInfo: fmt.Sprintf("pool=%s network=%s base=%s", g.PoolName, g.NetworkName, g.baseVolumeLabel()),
	}, nil
}

func (g *InstanceGroup) Increase(ctx context.Context, delta int) (int, error) {
	if delta <= 0 {
		return 0, nil
	}

	conn, err := g.connect()
	if err != nil {
		return 0, err
	}
	defer closeConnect(conn)

	pool, err := conn.LookupStoragePoolByName(g.PoolName)
	if err != nil {
		return 0, fmt.Errorf("looking up storage pool %q: %w", g.PoolName, err)
	}
	defer pool.Free()

	if err := pool.Refresh(0); err != nil {
		return 0, fmt.Errorf("refreshing storage pool %q: %w", g.PoolName, err)
	}

	network, err := conn.LookupNetworkByName(g.NetworkName)
	if err != nil {
		return 0, fmt.Errorf("looking up network %q: %w", g.NetworkName, err)
	}
	defer network.Free()

	baseVol, baseDetails, err := g.resolveBaseVolume(conn, pool)
	if err != nil {
		return 0, err
	}
	defer baseVol.Free()

	var errs []error
	created := 0
	for i := 0; i < delta; i++ {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}

		name, err := g.nextInstanceName()
		if err != nil {
			errs = append(errs, err)
			continue
		}

		if err := g.createInstance(conn, pool, baseDetails, name); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			continue
		}

		created++
		g.logger.Info("created libvirt instance", "instance", name)
	}

	return created, errors.Join(errs...)
}

func (g *InstanceGroup) Decrease(ctx context.Context, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	conn, err := g.connect()
	if err != nil {
		return nil, err
	}
	defer closeConnect(conn)

	pool, err := conn.LookupStoragePoolByName(g.PoolName)
	if err != nil {
		return nil, fmt.Errorf("looking up storage pool %q: %w", g.PoolName, err)
	}
	defer pool.Free()

	var (
		succeeded []string
		errs      []error
	)

	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}

		g.markDeleting(id)
		if err := g.deleteInstance(conn, pool, id); err != nil {
			g.unmarkDeleting(id)
			errs = append(errs, fmt.Errorf("%s: %w", id, err))
			continue
		}

		succeeded = append(succeeded, id)
	}

	return succeeded, errors.Join(errs...)
}

func (g *InstanceGroup) Update(ctx context.Context, update func(string, provider.State)) error {
	conn, err := g.connect()
	if err != nil {
		return err
	}
	defer closeConnect(conn)

	domains, err := conn.ListAllDomains(0)
	if err != nil {
		return fmt.Errorf("listing libvirt domains: %w", err)
	}

	deleting := g.snapshotDeleting()
	seen := make(map[string]struct{})

	for i := range domains {
		if err := ctx.Err(); err != nil {
			return err
		}

		dom := &domains[i]
		name, err := dom.GetName()
		if err != nil {
			_ = dom.Free()
			return fmt.Errorf("reading domain name: %w", err)
		}

		if !g.managesDomain(name) {
			_ = dom.Free()
			continue
		}

		seen[name] = struct{}{}
		state, err := g.currentState(ctx, conn, dom, deleting[name])
		_ = dom.Free()
		if err != nil {
			g.logger.Warn("failed to refresh domain state", "instance", name, "err", err)
			state = provider.StateTimeout
		}

		update(name, state)
	}

	g.pruneDeleting(seen)
	return nil
}

func (g *InstanceGroup) ConnectInfo(ctx context.Context, id string) (provider.ConnectInfo, error) {
	conn, err := g.connect()
	if err != nil {
		return provider.ConnectInfo{}, err
	}
	defer closeConnect(conn)

	dom, err := conn.LookupDomainByName(id)
	if err != nil {
		return provider.ConnectInfo{}, fmt.Errorf("looking up domain %q: %w", id, err)
	}
	defer dom.Free()

	address, err := g.waitForAddress(ctx, conn, dom, defaultConnectInfoWait)
	if err != nil {
		return provider.ConnectInfo{}, err
	}

	cfg := g.settings.ConnectorConfig
	return provider.ConnectInfo{
		ConnectorConfig: cfg,
		ID:              id,
		ExternalAddr:    address,
		InternalAddr:    address,
	}, nil
}

func (g *InstanceGroup) Shutdown(ctx context.Context) error {
	if g.logger != nil {
		g.logger.Info("libvirt plugin shutdown requested")
	}

	conn, err := g.connect()
	if err != nil {
		return err
	}
	defer closeConnect(conn)

	pool, err := conn.LookupStoragePoolByName(g.PoolName)
	if err != nil {
		return fmt.Errorf("looking up storage pool %q: %w", g.PoolName, err)
	}
	defer pool.Free()

	domains, err := conn.ListAllDomains(0)
	if err != nil {
		return fmt.Errorf("listing libvirt domains: %w", err)
	}

	var errs []error
	for i := range domains {
		dom := &domains[i]
		name, err := dom.GetName()
		if err != nil {
			_ = dom.Free()
			errs = append(errs, fmt.Errorf("reading domain name: %w", err))
			continue
		}

		if !g.managesDomain(name) {
			_ = dom.Free()
			continue
		}
		_ = dom.Free()

		if g.logger != nil {
			g.logger.Info("deleting instance on shutdown", "instance", name)
		}
		if err := g.deleteInstance(conn, pool, name); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
	}

	return errors.Join(errs...)
}

func (g *InstanceGroup) Heartbeat(ctx context.Context, id string) error {
	conn, err := g.connect()
	if err != nil {
		return err
	}
	defer closeConnect(conn)

	dom, err := conn.LookupDomainByName(id)
	if err != nil {
		if errors.Is(err, libvirt.ERR_NO_DOMAIN) {
			return provider.ErrInstanceUnhealthy
		}
		return fmt.Errorf("looking up domain %q: %w", id, err)
	}
	defer dom.Free()

	state, _, err := dom.GetState()
	if err != nil {
		return fmt.Errorf("getting state for %q: %w", id, err)
	}
	if state != libvirt.DOMAIN_RUNNING && state != libvirt.DOMAIN_BLOCKED {
		return provider.ErrInstanceUnhealthy
	}

	address, err := g.discoverAddress(ctx, conn, dom)
	if err != nil {
		return provider.ErrInstanceUnhealthy
	}

	dialCtx, cancel := context.WithTimeout(ctx, defaultHeartbeatTimeout)
	defer cancel()

	target := net.JoinHostPort(address, fmt.Sprintf("%d", g.settings.ProtocolPort))
	connCheck, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", target)
	if err != nil {
		return provider.ErrInstanceUnhealthy
	}
	_ = connCheck.Close()

	return nil
}

func (g *InstanceGroup) connect() (*libvirt.Connect, error) {
	conn, err := libvirt.NewConnect(g.URI)
	if err != nil {
		return nil, fmt.Errorf("connecting to libvirt at %q: %w", g.URI, err)
	}
	return conn, nil
}

func (g *InstanceGroup) validateLibvirtResources(conn *libvirt.Connect) error {
	pool, err := conn.LookupStoragePoolByName(g.PoolName)
	if err != nil {
		return fmt.Errorf("looking up storage pool %q: %w", g.PoolName, err)
	}
	defer pool.Free()

	if err := pool.Refresh(0); err != nil {
		return fmt.Errorf("refreshing storage pool %q: %w", g.PoolName, err)
	}

	network, err := conn.LookupNetworkByName(g.NetworkName)
	if err != nil {
		return fmt.Errorf("looking up network %q: %w", g.NetworkName, err)
	}
	defer network.Free()

	baseVol, _, err := g.resolveBaseVolume(conn, pool)
	if err != nil {
		return err
	}
	defer baseVol.Free()

	return nil
}

func (g *InstanceGroup) resolveBaseVolume(conn *libvirt.Connect, pool *libvirt.StoragePool) (*libvirt.StorageVol, baseVolumeDetails, error) {
	var (
		vol *libvirt.StorageVol
		err error
	)

	switch {
	case g.BaseVolumePath != "":
		vol, err = conn.LookupStorageVolByPath(g.BaseVolumePath)
	case g.BaseVolumeName != "":
		vol, err = pool.LookupStorageVolByName(g.BaseVolumeName)
	default:
		err = fmt.Errorf("either base_volume_name or base_volume_path must be set")
	}
	if err != nil {
		return nil, baseVolumeDetails{}, fmt.Errorf("looking up base flatcar volume: %w", err)
	}

	info, err := vol.GetInfo()
	if err != nil {
		_ = vol.Free()
		return nil, baseVolumeDetails{}, fmt.Errorf("reading base volume info: %w", err)
	}

	desc, err := vol.GetXMLDesc(0)
	if err != nil {
		_ = vol.Free()
		return nil, baseVolumeDetails{}, fmt.Errorf("reading base volume XML: %w", err)
	}

	format := volumeFormatFromXML(desc)
	if format == "" {
		format = "qcow2"
	}

	path, err := vol.GetPath()
	if err != nil {
		_ = vol.Free()
		return nil, baseVolumeDetails{}, fmt.Errorf("reading base volume path: %w", err)
	}

	return vol, baseVolumeDetails{
		Capacity: info.Capacity,
		Format:   format,
		Path:     path,
	}, nil
}

func (g *InstanceGroup) createInstance(conn *libvirt.Connect, pool *libvirt.StoragePool, baseDetails baseVolumeDetails, name string) error {
	ignition, err := g.renderIgnition(name)
	if err != nil {
		return fmt.Errorf("rendering ignition: %w", err)
	}

	ignitionPath := g.ignitionPath(name)
	if err := writeFileAtomic(ignitionPath, ignition, 0o644); err != nil {
		return fmt.Errorf("writing ignition file: %w", err)
	}

	volume, err := pool.StorageVolCreateXML(renderVolumeOverlayXML(g.volumeName(name), baseDetails), 0)
	if err != nil {
		_ = os.Remove(ignitionPath)
		return fmt.Errorf("creating overlay volume: %w", err)
	}
	defer volume.Free()

	if resize := g.diskSizeBytes(); resize > 0 && resize > baseDetails.Capacity {
		if err := volume.Resize(resize, 0); err != nil {
			_ = volume.Delete(0)
			_ = os.Remove(ignitionPath)
			return fmt.Errorf("resizing cloned volume: %w", err)
		}
	}

	diskPath, err := volume.GetPath()
	if err != nil {
		_ = volume.Delete(0)
		_ = os.Remove(ignitionPath)
		return fmt.Errorf("reading cloned volume path: %w", err)
	}

	macAddress, err := newMACAddress()
	if err != nil {
		_ = volume.Delete(0)
		_ = os.Remove(ignitionPath)
		return fmt.Errorf("generating mac address: %w", err)
	}

	domainXML, err := renderDomainXML(domainTemplateData{
		DomainType:    g.DomainType,
		Name:          name,
		Description:   managedDescriptionPrefix + ":" + g.DomainPrefix,
		MemoryMiB:     g.MemoryMiB,
		VCPUCount:     g.VCPUCount,
		Arch:          libvirtArch(g.settings.Arch),
		MachineType:   g.MachineType,
		DiskFormat:    baseDetails.Format,
		DiskPath:      diskPath,
		MACAddress:    macAddress,
		NetworkName:   g.NetworkName,
		FWCfgArgument: fmt.Sprintf("name=%s,file=%s", flatcarIgnitionFWCfgName, ignitionPath),
	})
	if err != nil {
		_ = volume.Delete(0)
		_ = os.Remove(ignitionPath)
		return err
	}

	domain, err := conn.DomainDefineXML(domainXML)
	if err != nil {
		_ = volume.Delete(0)
		_ = os.Remove(ignitionPath)
		return fmt.Errorf("defining libvirt domain: %w", err)
	}
	defer domain.Free()

	if err := domain.Create(); err != nil {
		_ = domain.Undefine()
		_ = volume.Delete(0)
		_ = os.Remove(ignitionPath)
		return fmt.Errorf("starting libvirt domain: %w", err)
	}

	return nil
}

func (g *InstanceGroup) deleteInstance(conn *libvirt.Connect, pool *libvirt.StoragePool, id string) error {
	var errs []error

	domain, err := conn.LookupDomainByName(id)
	if err == nil {
		active, activeErr := domain.IsActive()
		if activeErr != nil {
			errs = append(errs, fmt.Errorf("checking whether domain is active: %w", activeErr))
		} else if active {
			if err := domain.DestroyFlags(libvirt.DOMAIN_DESTROY_GRACEFUL); err != nil {
				if err := domain.Destroy(); err != nil && !errors.Is(err, libvirt.ERR_OPERATION_INVALID) && !errors.Is(err, libvirt.ERR_NO_DOMAIN) {
					errs = append(errs, fmt.Errorf("destroying domain: %w", err))
				}
			}
		}

		if err := domain.Undefine(); err != nil && !errors.Is(err, libvirt.ERR_OPERATION_INVALID) && !errors.Is(err, libvirt.ERR_NO_DOMAIN) {
			errs = append(errs, fmt.Errorf("undefining domain: %w", err))
		}

		_ = domain.Free()
	} else if !errors.Is(err, libvirt.ERR_NO_DOMAIN) {
		errs = append(errs, fmt.Errorf("looking up domain: %w", err))
	}

	if err := pool.Refresh(0); err != nil {
		errs = append(errs, fmt.Errorf("refreshing storage pool: %w", err))
	}

	volume, err := pool.LookupStorageVolByName(g.volumeName(id))
	if err == nil {
		if err := volume.Delete(0); err != nil && !errors.Is(err, libvirt.ERR_NO_STORAGE_VOL) {
			errs = append(errs, fmt.Errorf("deleting storage volume: %w", err))
		}
		_ = volume.Free()
	} else if !errors.Is(err, libvirt.ERR_NO_STORAGE_VOL) {
		errs = append(errs, fmt.Errorf("looking up storage volume: %w", err))
	}

	if err := os.Remove(g.ignitionPath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		errs = append(errs, fmt.Errorf("removing ignition file: %w", err))
	}

	return errors.Join(errs...)
}

func (g *InstanceGroup) currentState(ctx context.Context, conn *libvirt.Connect, dom *libvirt.Domain, deleting bool) (provider.State, error) {
	if deleting {
		return provider.StateDeleting, nil
	}

	state, _, err := dom.GetState()
	if err != nil {
		return provider.StateTimeout, err
	}

	switch state {
	case libvirt.DOMAIN_RUNNING, libvirt.DOMAIN_BLOCKED:
		if _, err := g.discoverAddress(ctx, conn, dom); err != nil {
			return provider.StateCreating, nil
		}
		return provider.StateRunning, nil
	case libvirt.DOMAIN_NOSTATE:
		return provider.StateCreating, nil
	case libvirt.DOMAIN_PAUSED, libvirt.DOMAIN_SHUTDOWN, libvirt.DOMAIN_SHUTOFF, libvirt.DOMAIN_CRASHED, libvirt.DOMAIN_PMSUSPENDED:
		return provider.StateTimeout, nil
	default:
		return provider.StateCreating, nil
	}
}

func (g *InstanceGroup) discoverAddress(ctx context.Context, conn *libvirt.Connect, dom *libvirt.Domain) (string, error) {
	switch g.AddressSource {
	case "lease":
		return g.addressFromLeaseSources(ctx, conn, dom)
	case "agent":
		return g.addressFromDomainSource(ctx, dom, libvirt.DOMAIN_INTERFACE_ADDRESSES_SRC_AGENT)
	case "arp":
		return g.addressFromDomainSource(ctx, dom, libvirt.DOMAIN_INTERFACE_ADDRESSES_SRC_ARP)
	case "auto":
		fallthrough
	default:
		if addr, err := g.addressFromLeaseSources(ctx, conn, dom); err == nil {
			return addr, nil
		}
		if addr, err := g.addressFromDomainSource(ctx, dom, libvirt.DOMAIN_INTERFACE_ADDRESSES_SRC_AGENT); err == nil {
			return addr, nil
		}
		return g.addressFromDomainSource(ctx, dom, libvirt.DOMAIN_INTERFACE_ADDRESSES_SRC_ARP)
	}
}

func (g *InstanceGroup) addressFromLeaseSources(ctx context.Context, conn *libvirt.Connect, dom *libvirt.Domain) (string, error) {
	if addr, err := g.addressFromDomainSource(ctx, dom, libvirt.DOMAIN_INTERFACE_ADDRESSES_SRC_LEASE); err == nil {
		return addr, nil
	}
	return g.addressFromNetworkLeases(ctx, conn, dom)
}

func (g *InstanceGroup) addressFromDomainSource(ctx context.Context, dom *libvirt.Domain, source libvirt.DomainInterfaceAddressesSource) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	ifaces, err := dom.ListAllInterfaceAddresses(source)
	if err != nil {
		return "", err
	}

	for _, iface := range ifaces {
		if addr := selectAddressFromInterfaces([]libvirt.DomainInterface{iface}); addr != "" {
			return addr, nil
		}
	}

	return "", fmt.Errorf("no address reported from source %d", source)
}

func (g *InstanceGroup) addressFromNetworkLeases(ctx context.Context, conn *libvirt.Connect, dom *libvirt.Domain) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	macs, err := domainMACAddresses(dom)
	if err != nil {
		return "", err
	}
	if len(macs) == 0 {
		return "", fmt.Errorf("domain has no interfaces with mac addresses")
	}

	network, err := conn.LookupNetworkByName(g.NetworkName)
	if err != nil {
		return "", fmt.Errorf("looking up network %q: %w", g.NetworkName, err)
	}
	defer network.Free()

	leases, err := network.GetDHCPLeases()
	if err != nil {
		return "", fmt.Errorf("reading dhcp leases for %q: %w", g.NetworkName, err)
	}

	var fallback string
	for _, lease := range leases {
		for _, mac := range macs {
			if !strings.EqualFold(mac, lease.Mac) {
				continue
			}
			if ip := normalizeDiscoveredIP(lease.IPaddr); ip != "" {
				if lease.Type == libvirt.IP_ADDR_TYPE_IPV4 {
					return ip, nil
				}
				if fallback == "" {
					fallback = ip
				}
			}
		}
	}

	if fallback != "" {
		return fallback, nil
	}

	return "", fmt.Errorf("no DHCP lease found for domain")
}

func (g *InstanceGroup) waitForAddress(ctx context.Context, conn *libvirt.Connect, dom *libvirt.Domain, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		address, err := g.discoverAddress(ctx, conn, dom)
		if err == nil {
			return address, nil
		}

		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("timed out waiting for a libvirt-discovered address for domain")
		}

		timer := time.NewTimer(time.Second)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return "", ctx.Err()
		}
	}
}

func (g *InstanceGroup) renderIgnition(hostname string) ([]byte, error) {
	user := ignitionUser{
		Name:              g.settings.Username,
		PasswordHash:      g.passwordHash,
		SSHAuthorizedKeys: append([]string(nil), g.authorizedKeys...),
	}
	if g.settings.Username != "core" {
		user.Groups = []string{"sudo", "docker"}
	}

	files := []ignitionFile{
		{
			Path:      "/etc/hostname",
			Mode:      intPtr(0o644),
			Overwrite: boolPtr(true),
			Contents: ignitionFileContents{
				Source: dataURL(hostname + "\n"),
			},
		},
	}
	files = append(files, g.caCertificateFiles...)

	units := []ignitionUnit{
		{
			Name:    "docker.service",
			Enabled: boolPtr(true),
		},
	}
	if len(g.caCertificateFiles) > 0 {
		units = append([]ignitionUnit{customCAUpdateIgnitionUnit()}, units...)
	}

	cfg := ignitionConfig{
		Ignition: ignitionSection{Version: ignitionVersion},
		Passwd:   ignitionPasswd{Users: []ignitionUser{user}},
		Storage: ignitionStorage{
			Files: files,
		},
		Systemd: ignitionSystemd{
			Units: units,
		},
	}

	return json.MarshalIndent(cfg, "", "  ")
}

func (g *InstanceGroup) normalizedConnectorConfig(cfg provider.ConnectorConfig) provider.ConnectorConfig {
	if cfg.Protocol == "" {
		cfg.Protocol = provider.ProtocolSSH
	}
	if cfg.ProtocolPort == 0 {
		cfg.ProtocolPort = defaultProtocolPort
	}
	if cfg.OS == "" {
		cfg.OS = "linux"
	}
	if cfg.Arch == "" {
		cfg.Arch = "amd64"
	}
	cfg.UseStaticCredentials = true
	return cfg
}

func (g *InstanceGroup) applyDefaults() {
	if g.URI == "" {
		g.URI = defaultURI
	}
	if g.NetworkName == "" {
		g.NetworkName = defaultNetworkName
	}
	if g.DomainPrefix == "" {
		g.DomainPrefix = defaultDomainPrefix
	}
	if g.StateDir == "" {
		g.StateDir = defaultStateDir
	}
	if g.DomainType == "" {
		g.DomainType = defaultDomainType
	}
	if g.AddressSource == "" {
		g.AddressSource = defaultAddressSource
	}
	if g.VCPUCount == 0 {
		g.VCPUCount = defaultVCPUCount
	}
	if g.MemoryMiB == 0 {
		g.MemoryMiB = defaultMemoryMiB
	}

	g.DomainPrefix = sanitizeDomainPrefix(g.DomainPrefix)
}

func (g *InstanceGroup) validateConfig() error {
	if g.MaxSize <= 0 {
		return fmt.Errorf("max_size must be greater than zero")
	}
	if strings.TrimSpace(g.PoolName) == "" {
		return fmt.Errorf("pool_name must be set")
	}
	if strings.TrimSpace(g.BaseVolumeName) == "" && strings.TrimSpace(g.BaseVolumePath) == "" {
		return fmt.Errorf("either base_volume_name or base_volume_path must be set")
	}
	if strings.TrimSpace(g.StateDir) == "" {
		return fmt.Errorf("state_dir must be set")
	}
	switch g.AddressSource {
	case "auto", "lease", "agent", "arp":
	default:
		return fmt.Errorf("address_source must be one of auto, lease, agent, arp")
	}
	return nil
}

func (g *InstanceGroup) diskSizeBytes() uint64 {
	if g.DiskSizeGiB == 0 {
		return 0
	}
	return uint64(g.DiskSizeGiB) * 1024 * 1024 * 1024
}

func (g *InstanceGroup) baseVolumeLabel() string {
	if g.BaseVolumeName != "" {
		return g.BaseVolumeName
	}
	return g.BaseVolumePath
}

func (g *InstanceGroup) managesDomain(name string) bool {
	return strings.HasPrefix(name, g.DomainPrefix+"-")
}

func (g *InstanceGroup) volumeName(id string) string {
	return id + ".img"
}

func (g *InstanceGroup) ignitionPath(id string) string {
	return filepath.Join(g.StateDir, id+".ign")
}

func (g *InstanceGroup) nextInstanceName() (string, error) {
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		return "", fmt.Errorf("reading random bytes for instance name: %w", err)
	}
	return fmt.Sprintf("%s-%s-%s", g.DomainPrefix, time.Now().UTC().Format("20060102-150405"), hex.EncodeToString(suffix)), nil
}

func (g *InstanceGroup) snapshotDeleting() map[string]bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	out := make(map[string]bool, len(g.deleting))
	for name := range g.deleting {
		out[name] = true
	}
	return out
}

func (g *InstanceGroup) markDeleting(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.deleting == nil {
		g.deleting = make(map[string]time.Time)
	}
	g.deleting[id] = time.Now()
}

func (g *InstanceGroup) unmarkDeleting(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.deleting, id)
}

func (g *InstanceGroup) pruneDeleting(seen map[string]struct{}) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for name := range g.deleting {
		if _, ok := seen[name]; !ok {
			delete(g.deleting, name)
		}
	}
}

func renderDomainXML(data domainTemplateData) (string, error) {
	var buf bytes.Buffer
	if err := domainXMLTmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("rendering domain XML: %w", err)
	}
	return strings.TrimSpace(buf.String()), nil
}

func renderVolumeOverlayXML(name string, details baseVolumeDetails) string {
	backingFormat := details.Format
	if backingFormat == "" {
		backingFormat = "qcow2"
	}
	return fmt.Sprintf(
		"<volume><name>%s</name><capacity unit='bytes'>%d</capacity><target><format type='qcow2'/></target><backingStore><path>%s</path><format type='%s'/></backingStore></volume>",
		escapeXML(name),
		details.Capacity,
		escapeXML(details.Path),
		escapeXML(backingFormat),
	)
}

func volumeFormatFromXML(desc string) string {
	var vol storageVolumeXML
	if err := xml.Unmarshal([]byte(desc), &vol); err != nil {
		return ""
	}
	return vol.Target.Format.Type
}

func volumeBackingStoreFormatFromXML(desc string) string {
	var vol storageVolumeXML
	if err := xml.Unmarshal([]byte(desc), &vol); err != nil {
		return ""
	}
	return vol.BackingStore.Format.Type
}

func volumeBackingStorePathFromXML(desc string) string {
	var vol storageVolumeXML
	if err := xml.Unmarshal([]byte(desc), &vol); err != nil {
		return ""
	}
	return vol.BackingStore.Path
}

func domainMACAddresses(dom *libvirt.Domain) ([]string, error) {
	desc, err := dom.GetXMLDesc(0)
	if err != nil {
		return nil, fmt.Errorf("reading domain XML: %w", err)
	}

	var parsed libvirtDomainXML
	if err := xml.Unmarshal([]byte(desc), &parsed); err != nil {
		return nil, fmt.Errorf("parsing domain XML: %w", err)
	}

	var macs []string
	for _, iface := range parsed.Devices.Interfaces {
		if iface.MAC.Address != "" {
			macs = append(macs, iface.MAC.Address)
		}
	}
	return macs, nil
}

func selectAddressFromInterfaces(ifaces []libvirt.DomainInterface) string {
	var fallback string
	for _, iface := range ifaces {
		for _, addr := range iface.Addrs {
			ip := normalizeDiscoveredIP(addr.Addr)
			if ip == "" {
				continue
			}

			if addr.Type == libvirt.IP_ADDR_TYPE_IPV4 {
				return ip
			}
			if fallback == "" {
				fallback = ip
			}
		}
	}
	return fallback
}

func normalizeDiscoveredIP(raw string) string {
	ip := net.ParseIP(strings.TrimSpace(raw))
	switch {
	case ip == nil:
		return ""
	case ip.IsLoopback():
		return ""
	case ip.IsLinkLocalUnicast():
		return ""
	case ip.IsLinkLocalMulticast():
		return ""
	default:
		return ip.String()
	}
}

func libvirtArch(arch string) string {
	switch strings.ToLower(strings.TrimSpace(arch)) {
	case "", "amd64", "x86_64":
		return "x86_64"
	case "arm64", "aarch64":
		return "aarch64"
	default:
		return arch
	}
}

func sanitizeDomainPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	prefix = strings.ToLower(prefix)
	prefix = domainPrefixSanitizer.ReplaceAllString(prefix, "-")
	prefix = strings.Trim(prefix, "-")
	if prefix == "" {
		return defaultDomainPrefix
	}
	return prefix
}

func newMACAddress() (string, error) {
	buf := make([]byte, 3)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return fmt.Sprintf("52:54:00:%02x:%02x:%02x", buf[0], buf[1], buf[2]), nil
}

func authorizedKeysFromPrivateKey(privateKey []byte) ([]string, error) {
	signer, err := ssh.ParsePrivateKey(privateKey)
	if err != nil {
		return nil, err
	}
	return []string{strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signer.PublicKey())))}, nil
}

func loadCACertificateIgnitionFiles(sourcePath string) ([]ignitionFile, error) {
	sourcePath = strings.TrimSpace(sourcePath)
	if sourcePath == "" {
		return nil, nil
	}

	hostPaths, err := caCertificateHostPaths(sourcePath)
	if err != nil {
		return nil, err
	}

	files := make([]ignitionFile, 0, len(hostPaths))
	usedNames := make(map[string]int, len(hostPaths))
	for i, hostPath := range hostPaths {
		content, err := os.ReadFile(hostPath)
		if err != nil {
			return nil, fmt.Errorf("reading %q: %w", hostPath, err)
		}
		if err := validatePEMCertificateBundle(content); err != nil {
			return nil, fmt.Errorf("validating %q: %w", hostPath, err)
		}

		name := uniqueCACertificateFileName(sanitizeCACertificateFileName(filepath.Base(hostPath), i), usedNames)
		files = append(files, ignitionFile{
			Path:      filepath.ToSlash(filepath.Join("/etc/ssl/certs", name)),
			Mode:      intPtr(0o644),
			Overwrite: boolPtr(true),
			Contents: ignitionFileContents{
				Source: dataURL(string(content)),
			},
		})
	}

	return files, nil
}

func caCertificateHostPaths(sourcePath string) ([]string, error) {
	info, err := os.Stat(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("stat %q: %w", sourcePath, err)
	}

	if info.Mode().IsRegular() {
		return []string{sourcePath}, nil
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%q must be a regular file or directory", sourcePath)
	}

	entries, err := os.ReadDir(sourcePath)
	if err != nil {
		return nil, fmt.Errorf("reading directory %q: %w", sourcePath, err)
	}

	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		entryPath := filepath.Join(sourcePath, entry.Name())
		entryInfo, err := os.Stat(entryPath)
		if err != nil {
			return nil, fmt.Errorf("stat %q: %w", entryPath, err)
		}
		if entryInfo.Mode().IsRegular() {
			paths = append(paths, entryPath)
		}
	}

	sort.Strings(paths)
	if len(paths) == 0 {
		return nil, fmt.Errorf("no certificate files found in %q", sourcePath)
	}

	return paths, nil
}

func validatePEMCertificateBundle(content []byte) error {
	rest := bytes.TrimSpace(content)
	if len(rest) == 0 {
		return fmt.Errorf("file is empty")
	}

	for len(rest) > 0 {
		block, remaining := pem.Decode(rest)
		if block == nil {
			return fmt.Errorf("file does not contain valid PEM-encoded certificates")
		}
		if block.Type != "CERTIFICATE" {
			return fmt.Errorf("unsupported PEM block type %q", block.Type)
		}
		if _, err := x509.ParseCertificates(block.Bytes); err != nil {
			return fmt.Errorf("parsing certificate: %w", err)
		}

		rest = bytes.TrimSpace(remaining)
	}

	return nil
}

func sanitizeCACertificateFileName(name string, index int) string {
	stem := strings.TrimSuffix(name, filepath.Ext(name))
	stem = strings.TrimSpace(stem)
	stem = caFileNameSanitizer.ReplaceAllString(stem, "-")
	stem = strings.Trim(stem, "-.")
	if stem == "" {
		stem = fmt.Sprintf("custom-ca-%02d", index+1)
	}
	return stem + ".pem"
}

func uniqueCACertificateFileName(name string, used map[string]int) string {
	count := used[name]
	used[name] = count + 1
	if count == 0 {
		return name
	}

	stem := strings.TrimSuffix(name, filepath.Ext(name))
	ext := filepath.Ext(name)
	return fmt.Sprintf("%s-%d%s", stem, count+1, ext)
}

func customCAUpdateIgnitionUnit() ignitionUnit {
	return ignitionUnit{
		Name:    customCAUpdateUnitName,
		Enabled: boolPtr(true),
		Contents: strings.TrimSpace(`
[Unit]
Description=Refresh custom CA certificates for GitLab Runner
Before=docker.service

[Service]
Type=oneshot
ExecStart=/bin/sh -ec 'update-ca-certificates'
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
`) + "\n",
	}
}

func dataURL(contents string) string {
	return "data:," + url.PathEscape(contents)
}

func writeFileAtomic(path string, content []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-ignition-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(content); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	return os.Rename(tmp.Name(), path)
}

func formatLibvirtVersion(version uint32) string {
	return fmt.Sprintf("%d.%d.%d", version/1000000, (version/1000)%1000, version%1000)
}

func closeConnect(conn *libvirt.Connect) {
	if conn == nil {
		return
	}
	_, _ = conn.Close()
}

func escapeXML(input string) string {
	var buf bytes.Buffer
	_ = xml.EscapeText(&buf, []byte(input))
	return buf.String()
}

func boolPtr(v bool) *bool {
	return &v
}

func intPtr(v int) *int {
	return &v
}
