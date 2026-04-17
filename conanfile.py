from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMakeDeps, cmake_layout


class KoormaRecipe(ConanFile):
    name = "koorma"
    version = "0.0.1"
    license = "Apache-2.0"
    url = "https://github.com/hari/koorma"
    description = (
        "Lean reimplementation of MathWorks turtle_kv with full API, "
        "on-disk, and behavioral compatibility."
    )
    topics = ("database", "key-value", "embedded", "turtle_kv")

    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeToolchain", "CMakeDeps"

    exports_sources = (
        "CMakeLists.txt",
        "include/*",
        "src/*",
        "test/*",
        "DECISIONS.md",
        "FORMAT.md",
    )

    def requirements(self):
        self.requires("abseil/20250127.0")
        self.requires("liburing/[>=2.11 <3]")
        self.requires("spdlog/[>=1.14 <2]")
        self.requires("fmt/[>=10 <12]", override=True)

    def build_requirements(self):
        self.tool_requires("cmake/[>=3.25]")
        self.test_requires("gtest/[>=1.16 <2]")

    def validate(self):
        if self.settings.os != "Linux":
            raise Exception("koorma is Linux-only (liburing, io_uring)")
        if self.settings.compiler == "gcc" and int(str(self.settings.compiler.version)) < 13:
            raise Exception("gcc >= 13 required for C++23")
        if self.settings.compiler == "clang" and int(str(self.settings.compiler.version)) < 17:
            raise Exception("clang >= 17 required for C++23")

    def layout(self):
        cmake_layout(self)
