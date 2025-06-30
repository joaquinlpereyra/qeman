# QEMAN - Quick EMulator MANager

A manager for QEMU VMs and images.
Simplifies operations and avoids pitfalls.

### Features

- `new`: Create a new image and boot from an installer ISO.
- `run`: Launch a VM with optional 9p mount and post-run script.
- `fork`: Create an overlay image from a base QCOW2.
- `snap`: Internal QCOW2 snapshot commands: `create`, `list`, `apply`, `delete`.
- `kill`: Kill a running VM by name.
- `list images`: List all managed VM images.
- `list vms`: Show currently running VMs and their PIDs.
- Respects `~/.qeman/config.toml` for custom `qemu-img` and `qemu-system` paths.

### Example Usage

```sh
# Create a new VM image and boot into ISO
qeman new debian.qcow2 ./debian.iso

# Run the VM
qeman run debian.qcow2 --graphical

# Fork a new overlay
qeman fork debian.qcow2 dev.qcow2

# Create a snapshot
qeman snap create dev.qcow2 before-experiment

# Apply a snapshot
qeman snap apply dev.qcow2 before-experiment

# Delete a snapshot
qeman snap delete dev.qcow2 before-experiment

# Kill a running VM
qeman kill dev.qcow2

# List all VM images
qeman list images

# List running VMs and PIDs
qeman list vms



