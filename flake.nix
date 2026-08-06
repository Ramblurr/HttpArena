{
  description = "Local benchmark tooling for HttpArena";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    gcannon-src.url = "github:MDA2AV/gcannon";
    gcannon-src.flake = false;
  };

  outputs = { self, nixpkgs, gcannon-src, ... }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      mkPkgs = system: import nixpkgs { inherit system; };
      mkGcannon = system:
        let
          pkgs = mkPkgs system;
        in
        pkgs.stdenv.mkDerivation {
          pname = "gcannon";
          version = "unstable";
          src = gcannon-src;
          nativeBuildInputs = [ pkgs.gnumake ];
          buildInputs = [ pkgs.liburing ];
          postPatch = ''
            substituteInPlace Makefile --replace-fail "-march=native " ""
          '';
          makeFlags = [ "CC=cc" ];
          installPhase = ''
            install -Dm755 gcannon "$out/bin/gcannon"
          '';
          meta = with pkgs.lib; {
            mainProgram = "gcannon";
            platforms = platforms.linux;
          };
        };
    in
    {
      packages = forAllSystems (system:
        let
          gcannon = mkGcannon system;
        in
        {
          inherit gcannon;
          default = gcannon;
        });

      devShells = forAllSystems (system:
        let
          pkgs = mkPkgs system;
          gcannonPkg = self.packages.${system}.gcannon;
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              bash
              bc
              coreutils
              curl
              docker
              findutils
              gawk
              ghz
              git
              gnugrep
              gnused
              iproute2
              jq
              nghttp2
              python3
              util-linux
              which
              wrk
              gcannonPkg
            ];

            shellHook = ''
              export GCANNON="${pkgs.lib.getExe gcannonPkg}"
              export GCANNON_SRC="${gcannon-src}"
              export PORT="''${PORT:-18080}"
              export H2PORT="''${H2PORT:-18443}"

              echo "HttpArena dev shell ready"
              echo "  GCANNON=$GCANNON"
              echo "  GCANNON_SRC=$GCANNON_SRC"
              echo "  PORT=$PORT H2PORT=$H2PORT"
              echo "  benchmark-lite: ./scripts/benchmark-lite.sh <framework> [profile]"
            '';
          };
        });
    };
}
