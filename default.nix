with import <nixpkgs> { };

let
  pythonPackages = python310Packages;
in
pkgs.mkShell rec {
  name = "datagovpython";
  venvDir = "./.venv";
  buildInputs = [
    pythonPackages.python
    pythonPackages.venvShellHook
    pythonPackages.cython
    taglib
    openssl
    git
    libxml2
    libxslt
    libzip
    zlib
  ];

  # Run this command, only after creating the virtual environment
  postVenvCreation = ''
    export SOURCE_DATE_EPOCH=315532800
  '';

  # Now we can execute any commands within the virtual environment.
  # This is optional and can be left out to run pip manually.
  postShellHook = ''
    export SOURCE_DATE_EPOCH=315532800
  '';

  LD_LIBRARY_PATH = "${pkgs.stdenv.cc.cc.lib}/lib;${zlib}/lib";
}
