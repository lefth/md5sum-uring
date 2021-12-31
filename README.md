# md5sum-uring

This program is written as an example of how to use [io-uring](https://docs.rs/io-uring/latest/io_uring)
to read multiple files in Rust. At time of writing, that library's example scripts only show
single-file examples, and the purpose of io-uring is to read many files.

#### Installation:
Since io-uring is a kernel feature, md5sum-uring only works on Linux or WSL2
running a somewhat recent kernel. Install with cargo:
```
cargo install --git https://github.com/lefth/md5sum-uring
```
This project isn't intended to replace your system md5sum, so none of md5sum's flags are implemented.

#### Performance:
Performance testing should be done without files in cache:
```
sync; echo 3 | sudo tee /proc/sys/vm/drop_caches; time md5sum-uring files/*
sync; echo 3 | sudo tee /proc/sys/vm/drop_caches; time md5sum files/*
```
When run on many small files, this implementation runs faster than the naive rust implementation of md5sum
(run with --no-uring), and about the same speed as the official md5sum binary. WSL2 seems to be an
exception: this program runs at least 3x faster than the official md5sum binary. In other situations, the
performance is comparable.

#### USAGE:
```
    md5sum-uring [FLAGS] [files]...
```

#### FLAGS:
```
    -h, --help                 Prints help information
        --no-uring             Compute checksums without the io_uring feature
        --o-direct             Open files with the O_DIRECT flag for performance
        --pre-register-files   Use the io_uring feature of pre-registering files to be read before the read is requested
        --use-fixed-buffers    Use the io_uring feature of reading into fixed position buffers
    -V, --version              Prints version information
```

#### ARGS:
```
    <files>...
```

#### Cross compiling:
I use this project to test APIs on ARM. The target you need may be different from mine--
in particular, I use "musl" because my target system has musl-based libc and "hf" because the target has
hardware-based floating point. You can also set these variables in a config file. This is how I compile
the project:
```
CARGO_TARGET_ARMV7_UNKNOWN_LINUX_MUSLEABIHF_LINKER=arm-linux-musleabihf-gcc cargo build --target armv7-unknown-linux-musleabihf --release
```

This is how I build tests:
```
CARGO_TARGET_ARMV7_UNKNOWN_LINUX_MUSLEABIHF_LINKER=arm-linux-musleabihf-gcc cargo test --target=armv7-unknown-linux-musleabihf --no-run
```

#### Limitations:
Registering fixed buffers requires root permissions. To run the fixed buffer tests with root permissions:
```
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER='sudo -E' cargo test test_fixed_buffers
CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER='sudo -E' cargo test test_fixed_buffers_o_direct
```
(Replace "X86_64_UNKNOWN_LINUX_GNU" with your current platform if needed).

O_DIRECT does not work on some systems (where direct disk IO is unavailable), such as WSL2 virtualized
filesystems and loopback mounts like VeraCrypt and LUKS.


<!-- 
vim: textwidth=106 expandtab
-->
