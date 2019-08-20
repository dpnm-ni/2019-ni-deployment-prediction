Original repository at https://github.com/srcvirus/middlebox-placement
Minor adaptations, e.g., for extended logging, deterministic time constraints.

After cloning the above repository, change into the middlebox-placement folder, and apply the patch:

* git apply --stat ../patch_e4bd6e8_to_ni-version.patch
* git apply --check ../patch_e4bd6e8_to_ni-version.patch
* git am --signoff < ../patch_e4bd6e8_to_ni-version.patch

Compile by running "make dbg" in middlebox-placement/src.
