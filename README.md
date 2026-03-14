# gitlab-runner-virt-plugin

`gitlab-runner-virt-plugin` is a [Fleeting](https://gitlab.com/gitlab-org/fleeting/fleeting) provider for GitLab Runner `docker-autoscaler` setups backed by libvirt.

It does three main things:

1. Clones a Flatcar base image from a libvirt storage pool.
2. Generates an Ignition config from the Runner SSH connector settings.
3. Boots a libvirt VM and reports its SSH address back to Runner.

The generated Ignition config:

- creates or updates the SSH user from `connector_config.username`
- hashes `connector_config.password` into Ignition `passwordHash`
- derives and installs an SSH public key when `connector_config.key` is set
- enables `docker.service`
- sets `/etc/hostname` to the VM name

## Requirements

- A working local libvirt daemon, typically reachable as `qemu:///system`
- A Flatcar QEMU image already imported into a libvirt storage pool
- A libvirt network that hands out DHCP leases
- A `state_dir` path that is readable by the QEMU/libvirt host process

Notes:

- The plugin injects Ignition through QEMU `fw_cfg` using Flatcar's libvirt path `opt/org.flatcar-linux/config`.
- Address discovery defaults to libvirt lease lookup, then falls back to guest-agent and ARP based lookup.
- The implementation assumes the Runner can reach the guest IP that libvirt reports. With the default NAT network this usually means the Runner is running on the same host as libvirt.

## Plugin Config

Required fields:

- `max_size`
- `pool_name`
- `base_volume_name` or `base_volume_path`

Common fields:

```json
{
  "uri": "qemu:///system",
  "pool_name": "default",
  "base_volume_name": "flatcar_production_qemu_image.img",
  "network_name": "default",
  "state_dir": "/var/lib/libvirt/gitlab-runner-virt-plugin",
  "domain_prefix": "gitlab-runner",
  "max_size": 10,
  "vcpu_count": 2,
  "memory_mib": 4096,
  "disk_size_gib": 40,
  "address_source": "auto"
}
```

Supported fields:

- `uri`: libvirt connection URI, default `qemu:///system`
- `pool_name`: target storage pool for cloned runner disks
- `base_volume_name`: source Flatcar volume inside `pool_name`
- `base_volume_path`: source Flatcar volume path, alternative to `base_volume_name`
- `network_name`: libvirt network name, default `default`
- `state_dir`: where generated Ignition files are written, default `/var/lib/libvirt/gitlab-runner-virt-plugin`
- `domain_prefix`: prefix for managed domain names, default `gitlab-runner`
- `max_size`: maximum number of VMs Runner may request
- `vcpu_count`: vCPU count per VM, default `2`
- `memory_mib`: memory per VM in MiB, default `4096`
- `disk_size_gib`: optional disk resize target; if larger than the base image, the cloned disk is expanded
- `domain_type`: libvirt domain type, default `kvm`
- `machine_type`: optional machine type passed into the libvirt XML `os/type` element
- `address_source`: `auto`, `lease`, `agent`, or `arp`

## Runner Example

Example `config.toml` fragments for a GitLab Runner using `docker-autoscaler`:

```toml
[runners.autoscaler.plugin_config]
uri = "qemu:///system"
pool_name = "default"
base_volume_name = "flatcar_production_qemu_image.img"
network_name = "default"
state_dir = "/var/lib/libvirt/gitlab-runner-virt-plugin"
domain_prefix = "gitlab-runner"
max_size = 10
vcpu_count = 2
memory_mib = 4096
disk_size_gib = 40

[runners.autoscaler.connector_config]
os = "linux"
arch = "amd64"
protocol = "ssh"
protocol_port = 22
username = "core"
password = "super-secret-password"
```

If you use SSH keys instead of passwords, set `connector_config.key`. The plugin will derive the matching public key and install it via Ignition.

## Build

```bash
go build ./...
```

## Operational Notes

- Imported Flatcar images should be the official QEMU/libvirt-ready image format.
- `state_dir` must exist on the hypervisor host filesystem because libvirt passes the Ignition file to QEMU by host path.
- Managed instances are identified by the configured `domain_prefix`.
- The plugin deletes the libvirt domain definition, the cloned storage volume, and the generated Ignition file when Runner scales an instance down.

## References

- Flatcar libvirt provisioning: https://www.flatcar.org/docs/latest/installing/virtualization/libvirt/
- Flatcar authentication examples: https://www.flatcar.org/docs/latest/setup/customization/configuring-flatcar/
- libvirt domain XML and `fw_cfg`: https://libvirt.org/formatdomain.html
