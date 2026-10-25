#pragma once

#include <functional>
#include <iostream>
#include <string>
#include <vector>

// Minimal header-only test harness (no external deps) so the test binary builds
// and runs anywhere gRPC's toolchain is available.
namespace test {

inline int& failures() {
    static int f = 0;
    return f;
}

inline void check(bool cond, const std::string& name) {
    if (cond) {
        std::cout << "  PASS " << name << "\n";
    } else {
        std::cout << "  FAIL " << name << "\n";
        ++failures();
    }
}

struct Suite {
    std::string              name;
    std::function<void()>    fn;
};

inline std::vector<Suite>& suites() {
    static std::vector<Suite> s;
    return s;
}

struct Registrar {
    Registrar(const std::string& n, std::function<void()> f) {
        suites().push_back({n, std::move(f)});
    }
};

inline int run_all() {
    int passed = 0;
    for (auto& s : suites()) {
        std::cout << "[suite] " << s.name << "\n";
        s.fn();
        ++passed;
    }
    std::cout << "\n========================================\n";
    std::cout << "suites run: " << passed
              << "   failures: " << failures() << "\n";
    std::cout << (failures() == 0 ? "RESULT: OK\n" : "RESULT: FAILED\n");
    std::cout << "========================================\n";
    return failures() == 0 ? 0 : 1;
}

}  // namespace test

#define TEST(name)                                               \
    void name();                                                 \
    static test::Registrar _reg_##name(#name, name);             \
    void name()
