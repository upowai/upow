from fastecdsa import curve

ENDIAN = 'little'
CURVE = curve.P256
SMALLEST = 100000000
MAX_SUPPLY = 18_884_643.75
VERSION = 2
MAX_BLOCK_SIZE_HEX = 4096 * 1024  # 4MB in HEX format, 2MB in raw bytes
MAX_INODES = 12

SYNC_CHECKPOINTS = {
    # Block height -> block hash for fast-sync validation.
    # Blocks below the highest checkpoint skip signature verification and
    # UTXO double-spend queries, only checking hash chain continuity,
    # difficulty target, and merkle tree — dramatically faster initial sync.
    250000:  "0a39e296b9397b61177d3bc483dce1e0c4201af99ffb1935a4611aa1780577aa",
    500000:  "eca7cd65e01b2863ea87675ce1c9d75066fab9f91b478699473f02cc3bd119a2",
    750000:  "d2980d018bb2f551c3d41da1d2b4bd01bd967264424e92f19d10134d231c8baa",
    1000000: "7b3368bf9c28a1bff3d4c8d574ab661b10f62564f413754132448b3b97aba6a8",
    1250000: "2baf281764329e7c54041014d168cb2a693cf16c5afa5d97d2bcc3fc63fd4bad",
}
