{
  description = "ProsperOK Metrics Development Environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
    ojodb = {
      url = "github:openjusticeok/ojodb?ref=ojodb-v3";
      flake = false;
    };
    ojoutils = {
      url = "github:openjusticeok/ojoutils";
      flake = false;
    };
    ojothemes = {
      url = "github:openjusticeok/ojothemes";
      flake = false;
    };
    tulsa-county-jail-scraper = {
      url = "git+ssh://git@github.com/openjusticeok/tulsa-county-jail-scraper.git";
      flake = false;
    };
  };

  outputs = inputs@{ self, nixpkgs, flake-parts, ojodb, ojoutils, ojothemes, tulsa-county-jail-scraper }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" ];

      perSystem = { config, self', inputs', system, ... }:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [
              (final: prev: {
                arrow-cpp = prev.arrow-cpp.override {
                  enableGcs = true;
                  enableS3 = true;
                };
              })
            ];
            config = {
              permittedInsecurePackages = [
                "electron-38.8.4"
              ];
              problems.handlers = {
                googleCloudRunner.broken = "warn";
              };
            };
          };

          # Override arrow R package to properly detect GCS/S3 from arrow-cpp
          arrowWithGcs = pkgs.rPackages.arrow.overrideAttrs (oldAttrs: {
            ARROW_HOME = "${pkgs.arrow-cpp}";
            ARROW_USE_PKG_CONFIG = "true";
            LIBARROW_MINIMAL = "false";
            PKG_CONFIG_PATH = "${pkgs.arrow-cpp}/lib/pkgconfig";
            NIX_LDFLAGS = "-L${pkgs.arrow-cpp}/lib -larrow -larrow_compute -larrow_acero -larrow_dataset -lparquet";
            
            postPatch = (oldAttrs.postPatch or "") + ''
              echo "Bypassing regex hell by providing a patched ArrowOptions.cmake..."
              mkdir -p .nix-cmake
              sed 's/"TRUE"/"ON"/g' ${pkgs.arrow-cpp}/lib/cmake/Arrow/ArrowOptions.cmake > .nix-cmake/ArrowOptions.cmake
              sed -i 's|^ARROW_OPTS_CMAKE=.*|ARROW_OPTS_CMAKE=".nix-cmake/ArrowOptions.cmake"|g' configure
              sed -i 's|$ARROW_OPTS_CMAKE|.nix-cmake/ArrowOptions.cmake|g' configure
            '';
          });

          # Ojodb - installed from source
          ojodb-pkg = pkgs.rPackages.buildRPackage {
            name = "ojodb";
            src = ojodb;
            propagatedBuildInputs = with pkgs.rPackages; [
              dplyr
              dbplyr
              DBI
              digest
              RPostgres
              ggplot2
              pool
              rlang
              glue
              stringr
              purrr
              tidyr
              janitor
              lubridate
              hms
              fs
              yaml
            ];
          };

          # Ojoutils - installed from source
          ojoutils-pkg = pkgs.rPackages.buildRPackage {
            name = "ojoutils";
            src = ojoutils;
            nativeBuildInputs = with pkgs; [ cargo rustc ];
            postPatch = ''
              # Run config script manually to generate src/Makevars, then replace configure
              ${pkgs.R}/bin/Rscript tools/config.R || true
              cat > configure <<'EOF'
#!/bin/sh
# Nix stub: config.R already ran during postPatch
EOF
              chmod +x configure
              # Relax arrow version: nixpkgs has v20, ojoutils wants >=24
              # as_arrow_array() only requires >=12, so v20 is sufficient
              [ -f DESCRIPTION ] && sed -i 's/arrow (>= 24\.0\.0)/arrow (>= 12.0.0)/g' DESCRIPTION
            '';
            propagatedBuildInputs = with pkgs.rPackages; [
              arrowWithGcs
              cli
              dbplyr
              dplyr
              fs
              gargle
              gert
              gh
              glue
              googleCloudStorageR
              janitor
              lubridate
              nanoarrow
              readr
              renv
              rlang
              stringr
              targets
              tidyr
              usethis
              withr
            ];
          };

          # Ojothemes - installed from source
          ojothemes-pkg = pkgs.rPackages.buildRPackage {
            name = "ojothemes";
            src = ojothemes;
            postPatch = ''
              # Disable font downloads during build (network access not allowed in sandbox)
              # Replace .onAttach with a no-op during installation
              cat > R/zzz.R <<'EOFR'
.onAttach <- function(libname, pkgname) {
  # Font loading disabled during Nix build (no network access)
  # Fonts will be loaded at runtime when needed
  invisible(NULL)
}
EOFR
            '';
            propagatedBuildInputs = with pkgs.rPackages; [
              colorspace
              dplyr
              ggplot2
              gt
              lifecycle
              rlang
              showtext
              snakecase
              stringr
              sysfonts
              tidyselect
            ];
          };

          # tulsaCountyJailScraper - installed from source
          tulsa-county-jail-scraper-pkg = pkgs.rPackages.buildRPackage {
            name = "tulsaCountyJailScraper";
            src = tulsa-county-jail-scraper;
            postPatch = ''
              # Remove .Rprofile to prevent renv activation during build
              rm -f .Rprofile
            '';
            propagatedBuildInputs = with pkgs.rPackages; [
              checkmate
              dplyr
              fs
              glue
              httr2
              logger
              lubridate
              ojodb-pkg
              purrr
              rlang
            ];
          };

          # R packages to include in the wrapper
          rPackages = [
            ojodb-pkg
            ojoutils-pkg
            ojothemes-pkg
            tulsa-county-jail-scraper-pkg
            arrowWithGcs
          ] ++ (with pkgs.rPackages; [
            # Repo-specific dependencies (explicitly listed, independent of ojodb/ojoutils)
            targets
            tarchetypes
            qs2
            googleCloudStorageR
            googledrive
            httpuv
            dplyr
            fs
            stringr
            janitor
            purrr
            tibble
            rlang
            pointblank
            here
            quarto
            qs2
            gtExtras
            nanoparquet
            ggrepel
          ]);

          # R wrapper with all packages
          R = pkgs.rWrapper.override {
            packages = rPackages;
          };
          
          # R library path for tools that need it
          rLibsPath = pkgs.lib.makeLibraryPath rPackages;
        in
        {
          # Development shell
          devShells.default = pkgs.mkShell {
            name = "r-dev";

            buildInputs = [
              R
              pkgs.radian
              pkgs.air-formatter
              pkgs.jarl
              pkgs.quarto
              pkgs.rustc
              pkgs.cargo
            ];

            shellHook = ''
              export PATH="${R}/bin:$PATH"
              export R_LIBS_SITE="${rLibsPath}"
              export ARROW_HOME="${pkgs.arrow-cpp}"
              export LD_LIBRARY_PATH="${pkgs.arrow-cpp}/lib''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
              
              echo "🚀 R Development Environment"
              echo ""
              echo "Available tools:"
              echo "  - R (with ojodb, ojoutils, arrow-with-gcs)"
              echo "  - radian (enhanced R REPL)"
              echo "  - air (R formatter)"
              echo "  - jarl (R linter)"
              echo "  - quarto"
              echo "  - Rust (rustc, cargo)"
              echo ""
              echo "Verify Arrow GCS support:"
              echo "  R -e 'arrow::arrow_info()'"
              echo ""
            '';
          };

          # Packages exposed for inspection
          packages = {
            inherit R;
            air = pkgs.air-formatter;
            jarl = pkgs.jarl;
          };
        };
    };
}
