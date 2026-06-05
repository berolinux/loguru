"""Compile (once) and load the tiny C atomics shared library via ctypes."""

import ctypes
import os
import subprocess

_lib = None
_dir = os.path.dirname(os.path.abspath(__file__))
_c_src = os.path.join(_dir, "_atomic_ops.c")
_so_path = os.path.join(_dir, "_atomic_ops.so")


def _compile():
    try:
        subprocess.check_call(
            ["gcc", "-shared", "-fPIC", "-O2", "-o", _so_path, _c_src],
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        raise RuntimeError(
            "gcc is required to compile the atomics library. "
            "Install it with: apt-get install gcc"
        ) from None
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"Failed to compile {_c_src}: {exc.stderr.decode()}"
        ) from exc


def get_lib():
    """Return the loaded ctypes CDLL, compiling the .so if necessary."""
    global _lib
    if _lib is not None:
        return _lib

    need_compile = not os.path.exists(_so_path)
    if not need_compile and os.path.exists(_c_src):
        need_compile = os.path.getmtime(_c_src) > os.path.getmtime(_so_path)
    if need_compile:
        _compile()

    lib = ctypes.CDLL(_so_path)

    # -- uint64 operations --
    lib.rb_atomic_load_u64.restype = ctypes.c_uint64
    lib.rb_atomic_load_u64.argtypes = [ctypes.c_void_p]

    lib.rb_atomic_store_u64.restype = None
    lib.rb_atomic_store_u64.argtypes = [ctypes.c_void_p, ctypes.c_uint64]

    lib.rb_atomic_fetch_add_u64.restype = ctypes.c_uint64
    lib.rb_atomic_fetch_add_u64.argtypes = [ctypes.c_void_p, ctypes.c_uint64]

    lib.rb_atomic_cas_u64.restype = ctypes.c_uint64
    lib.rb_atomic_cas_u64.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint64,
        ctypes.c_uint64,
    ]

    # -- uint8 operations --
    lib.rb_atomic_load_u8.restype = ctypes.c_uint8
    lib.rb_atomic_load_u8.argtypes = [ctypes.c_void_p]

    lib.rb_atomic_store_u8.restype = None
    lib.rb_atomic_store_u8.argtypes = [ctypes.c_void_p, ctypes.c_uint8]

    # -- fence --
    lib.rb_memory_fence.restype = None
    lib.rb_memory_fence.argtypes = []

    _lib = lib
    return _lib
