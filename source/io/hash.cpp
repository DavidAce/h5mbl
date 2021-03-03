//
// Created by david on 2020-10-09.
//
#include <array>
#include <fstream>
#include <functional>
#include <h5pp/details/h5ppFilesystem.h>
#include <iostream>
#include <openssl/md5.h>
#include <openssl/sha.h>
#include <string>
namespace tools::hash {
    std::string sha256_file(const std::string &fn) {
        std::ifstream file(fn, std::ios::binary);
        if(file.is_open()) {
            std::array<char, 524288> buf = {};
            SHA256_CTX               sha256;
            SHA256_Init(&sha256);
            while(not file.eof()) {
                file.read(buf.data(), buf.size());
                std::streamsize len = file.gcount();
                if(len < 0) throw std::length_error("file streamsize len < 0");
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

    std::string md5_file(const std::string &fn) {
        std::ifstream file(fn, std::ios::binary);
        if(file.is_open()) {
            std::array<char, 524288> buf = {};
            MD5_CTX                  md5;
            MD5_Init(&md5);
            while(not file.eof()) {
                file.read(buf.data(), buf.size());
                std::streamsize len = file.gcount();
                if(len < 0) throw std::runtime_error("len < 0");
                MD5_Update(&md5, buf.data(), static_cast<size_t>(len));
            }
            std::array<unsigned char, MD5_DIGEST_LENGTH> hex = {};
            std::array<char, MD5_DIGEST_LENGTH * 2 + 1>  out = {};
            MD5_Final(hex.data(), &md5);
            for(size_t i = 0; i < MD5_DIGEST_LENGTH; i++) sprintf(out.data() + (i * 2), "%02x", hex[i]);
            out.back() = 0;
            //            for(auto &h : hex) snprintf(out.data(),out.size(), "%s%02x", out.data(), h);
            return std::string(out.begin(), out.end());
        } else {
            throw std::runtime_error("Failed to open file: " + fn);
        }
    }

    std::string md5_string(const std::string &str) {
        MD5_CTX md5;
        MD5_Init(&md5);
        MD5_Update(&md5, str.data(), str.size());
        std::array<unsigned char, MD5_DIGEST_LENGTH> hex = {};
        std::array<char, MD5_DIGEST_LENGTH * 2 + 1>  out = {};
        MD5_Final(hex.data(), &md5);
        for(size_t i = 0; i < MD5_DIGEST_LENGTH; i++) sprintf(out.data() + (i * 2), "%02x", hex[i]);
        out.back() = 0;
        return std::string(out.begin(), out.end());
    }

    std::string md5_file_meta(const h5pp::fs::path &fpath, const std::string &more_meta) {
        std::string meta;
        meta.reserve(512);
        meta += fpath.string() + '\n';
        meta += std::to_string(h5pp::fs::last_write_time(fpath).time_since_epoch().count()) + '\n';
        if(not more_meta.empty()) meta += more_meta + '\n';
        return std::to_string(std::hash<std::string>{}(meta));
        //        return md5_string(meta);
    }

}
