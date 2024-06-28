{
  description = "A clj-nix flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    clj-nix.url = "github:jlesquembre/clj-nix";
    devshell = {
      url = "github:numtide/devshell";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, clj-nix, ... }:

    flake-utils.lib.eachDefaultSystem (system: {

      devShells.default =
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in pkgs.mkShell {
          packages = [
            pkgs.openjdk17_headless
            pkgs.jq
            pkgs.clojure
            pkgs.openjdk
          ];
      };
      packages = {
        deps-lock = clj-nix.packages."${system}".deps-lock;
        default = clj-nix.lib.mkCljApp {
          pkgs = nixpkgs.legacyPackages.${system};
          modules = [
            # Option list:
            # https://jlesquembre.github.io/clj-nix/options/
            {
              projectSrc = ./.;
              name = "me.lafuente/cljdemo";
              main-ns = "hello.core";

              nativeImage.enable = true;

              # customJdk.enable = true;
            }
          ];
        };

      };
    });
}
