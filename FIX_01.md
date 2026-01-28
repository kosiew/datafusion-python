
The regression stems from the workflow/build steps installing **dev dependencies** in **free-threaded Python environments (3.13t/3.14t)**.

The dev group includes `pygithub`, which depends on `pynacl==1.5.0`. **PyNaCl does not currently provide wheels for free-threaded CPython**, and building from source requires `libsodium` tooling that isn’t available in the build image, causing the following command to fail:

```bash
uv run --no-project maturin build ... --interpreter python
```

---

## Fix plan

* Stop installing **dev dependencies** during wheel builds

  * Only install what’s needed to run `maturin`
* **OR** make `pygithub` optional / non-dev for build environments so it isn’t pulled into free-threaded builds
