//
// Created by david on 2020-10-09.
//
#include <array>
#include <fstream>
#include <iostream>
#include <openssl/sha.h>
#include <string>
namespace tools::hash {
    std::string sha256_file(const std::string &fn) {
        std::ifstream file(fn, std::ios::binary);
        if(file.is_open()) {
            std::array<char, 8192> buf = {};
            SHA256_CTX             sha256;
            SHA256_Init(&sha256);
            while(not file.eof()) {
                file.read(buf.data(), buf.size());
                std::streamsize len = file.gcount();
                if(len < 0) throw std::runtime_error("len < 0");
                SHA256_Update(&sha256, buf.data(), static_cast<size_t>(len));
            }
            std::array<unsigned char, SHA256_DIGEST_LENGTH> hex = {};
            std::array<char, SHA256_DIGEST_LENGTH * 2 + 1>  out = {};
            SHA256_Final(hex.data(), &sha256);
            for(size_t i = 0; i < SHA256_DIGEST_LENGTH; i++) sprintf(out.data() + (i * 2), "%02x", hex[i]);
            out.back() = 0;
            //            for(auto &h : hex) snprintf(out.data(),out.size(), "%s%02x", out.data(), h);
            return std::string(out.begin(), out.end());
        } else {
            throw std::runtime_error("Failed to open file: " + fn);
        }
    }
}
