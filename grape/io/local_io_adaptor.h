/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef GRAPE_IO_LOCAL_IO_ADAPTOR_H_
#define GRAPE_IO_LOCAL_IO_ADAPTOR_H_

#include <stdio.h>

#include <fstream>
#include <string>
#include <vector>
#ifdef WITH_HDFS
#include <hdfs.h>
#endif

#include "grape/io/io_adaptor_base.h"

namespace grape {
class InArchive;
class OutArchive;

/**
 * @brief A default adaptor to read/write files from local locations.
 *
 */
class LocalIOAdaptor : public IOAdaptorBase {
 public:
  explicit LocalIOAdaptor(std::string location);

  ~LocalIOAdaptor() override;

  void Open() override;

  void Open(const char* mode) override;

  void Close() override;

  bool Configure(const std::string& key, const std::string& value) override;

  bool SetPartialRead(int index, int total_parts) override;

  bool ReadLine(std::string& line) override;

  bool ReadArchive(OutArchive& archive) override;

  bool WriteArchive(InArchive& archive) override;

  bool Read(void* buffer, size_t size) override;

  bool Write(void* buffer, size_t size) override;

  void MakeDirectory(const std::string& path) override;

  bool IsExist() override;

 private:
  enum IOType { LOCAL, HDFS };  // 新增 IO 类型判断
  static constexpr size_t LINE_SIZE = 65535;
  static constexpr size_t HDFS_LINE_SIZE = 134217728;
  enum FileLocation {
    kFileLocationBegin = 0,
    kFileLocationCurrent = 1,
    kFileLocationEnd = 2,
  };

  int64_t tell();
  void seek(int64_t offset, FileLocation seek_from);
  bool setPartialReadImpl();

  // HDFS 专用辅助函数
  void hdfsOpen1(const char* mode);
  bool hdfsClose1();
  bool hdfsReadLine1(std::string& line);
  int64_t hdfsTell1();
  //bool hdfsReadArchive1(OutArchive& archive);
  //bool hdfsWriteArchive1(InArchive& archive);
  bool hdfsRead1(void* buffer, size_t size);
  bool hdfsFillBuffer();
  void hdfsSeek1(const int64_t offset, const FileLocation seek_from);
  // bool hdfsWrite1(void* buffer, size_t size);
  // bool hdfsMakeDirectory1(const std::string& path);
  // bool hdfsIsExist1();

  FILE* file_;
  std::fstream fs_;
  std::string location_;
  bool using_std_getline_;
  char buff[LINE_SIZE]{};

  bool enable_partial_read_;
  std::vector<int64_t> partial_read_offset_;
  int total_parts_;
  int index_;
  IOType io_type_ = LOCAL;           // 当前 IO 类型

  #ifdef WITH_HDFS
  // HDFS 专用成员
  hdfsFS hdfs_conn_ = nullptr;       // HDFS 连接句柄
  hdfsFile hdfs_file_ = nullptr;     // HDFS 文件句柄
  std::string hdfs_file_path_;  // HDFS 文件路径
  std::vector<char> buffer_;
  size_t buffer_size_ = 0;
  size_t buffer_pos_ = 0;
  #endif
};
}  // namespace grape

#endif  // GRAPE_IO_LOCAL_IO_ADAPTOR_H_
