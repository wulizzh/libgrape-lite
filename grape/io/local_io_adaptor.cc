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

#include "grape/io/local_io_adaptor.h"

#include <sys/stat.h>

#include <string>
#include <iostream>

#ifdef WITH_HDFS
#include <hdfs.h>
#endif

#include <glog/logging.h>

#include "grape/serialization/in_archive.h"
#include "grape/serialization/out_archive.h"

namespace grape {

LocalIOAdaptor::LocalIOAdaptor(std::string location)
    : file_(nullptr),
      location_(std::move(location)),
      using_std_getline_(false),
      enable_partial_read_(false),
      total_parts_(0),
      index_(0),
      io_type_(location_.find("hdfs") == 0 ? HDFS : LOCAL)
      {
        //std::cout << "LocalIOAdaptor::LocalIOAdaptor" << location_ << std::endl;
        #ifdef WITH_HDFS
        if (io_type_ == HDFS) {
            hdfs_file_path_ = location_.substr(4); // 去掉 "hdfs"
            // 初始化 HDFS 连接（这里使用默认配置，实际需支持配置参数）
            hdfs_conn_ = hdfsConnect("default", 0);  // 或从配置获取 namenode 信息
            CHECK(hdfs_conn_) << "Failed to connect to HDFS";
        }
        #endif
      }

LocalIOAdaptor::~LocalIOAdaptor() {
  //std::cout << "~LocalIOAdaptor"<< std::endl;
  if(io_type_ == LOCAL) {
    if (file_ != nullptr) {
      fclose(file_);
      file_ = nullptr;
    } else if (fs_.is_open()) {
      fs_.clear();
      fs_.close();
    }
  } else {
    #ifdef WITH_HDFS
    if (hdfs_conn_) {
      hdfsDisconnect(hdfs_conn_);
    }
    #endif
  }
}

int64_t LocalIOAdaptor::tell() {
  //std::cout << "nt64_t LocalIOAdaptor::tell()"<< std::endl;
  if(io_type_ == LOCAL){
    if (using_std_getline_) {
      return fs_.tellg();
    } else {
      return ftell(file_);
    }
  } else {
    #ifdef WITH_HDFS
    return hdfsTell1(); // 调用 HDFS 专用实现
    #else
    return -1;
    #endif
  }
  
}

void LocalIOAdaptor::seek(const int64_t offset, const FileLocation seek_from) {
  //std::cout << "LocalIOAdaptor::seek " << offset << " " << seek_from << std::endl;
  if(io_type_ == LOCAL) {
    if (using_std_getline_) {
      fs_.clear();
      if (seek_from == kFileLocationBegin) {
        fs_.seekg(offset, fs_.beg);
      } else if (seek_from == kFileLocationCurrent) {
        fs_.seekg(offset, fs_.cur);
      } else if (seek_from == kFileLocationEnd) {
        fs_.seekg(offset, fs_.end);
      } else {
        VLOG(1) << "invalid value, offset = " << offset
                << ", seek_from = " << seek_from;
      }
    } else {
      if (seek_from == kFileLocationBegin) {
        fseek(file_, offset, SEEK_SET);
      } else if (seek_from == kFileLocationCurrent) {
        fseek(file_, offset, SEEK_CUR);
      } else if (seek_from == kFileLocationEnd) {
        fseek(file_, offset, SEEK_END);
      } else {
        VLOG(1) << "invalid value, offset = " << offset
                << ", seek_from = " << seek_from;
      }
    }
  } else {
    #ifdef WITH_HDFS
    hdfsSeek1(offset, seek_from); // 调用 HDFS 专用实现
    #endif
  }
}

void LocalIOAdaptor::Open() {
  //std::cout << "LocalIOAdaptor::Open void" << std::endl; 
  return this->Open("r"); 
}

void LocalIOAdaptor::Open(const char* mode) {
  //std::cout << "LocalIOAdaptor::Open" << mode << std::endl;
  if(io_type_ == LOCAL) {
    std::string tag = ".gz";
    size_t pos = location_.find(tag);
    if (pos != location_.size() - tag.size()) {
      if (strchr(mode, 'w') != NULL || strchr(mode, 'a') != NULL) {
        int t = location_.find_last_of('/');
        if (t != -1) {
          std::string folder_path = location_.substr(0, t);
          if (access(folder_path.c_str(), 0) != 0) {
            MakeDirectory(folder_path);
          }
        }
      }
      if (using_std_getline_) {
        if (strchr(mode, 'b') != NULL) {
          fs_.open(location_.c_str(),
                  std::ios::binary | std::ios::in | std::ios::out);
        } else if (strchr(mode, 'a') != NULL) {
          fs_.open(location_.c_str(),
                  std::ios::out | std::ios::in | std::ios::app);
        } else if (strchr(mode, 'w') != NULL || strchr(mode, '+') != NULL) {
          fs_.open(location_.c_str(),
                  std::ios::out | std::ios::in | std::ios::trunc);
        } else if (strchr(mode, 'r') != NULL) {
          fs_.open(location_.c_str(), std::ios::in);
        }
      } else {
        file_ = fopen(location_.c_str(), mode);
      }
    } else {
      LOG(FATAL) << "invalid operation";
    }

    if ((using_std_getline_ && !fs_) ||
        (!using_std_getline_ && file_ == nullptr)) {
      LOG(FATAL) << "file doesn't exists. file = " << location_;
    }
  } else {
    #ifdef WITH_HDFS
    hdfsOpen1(mode);
    #endif
  }
  
  // check the partial read flag
  if (enable_partial_read_) {
    setPartialReadImpl();
  }
}

//sssp例子暂未用到
bool LocalIOAdaptor::Configure(const std::string& key, const std::string& value) {
  //std::cout << "LocalIOAdaptor::Configure" << key << " " << value << std::endl;
  if (key == "using_std_getline") {
    if (value == "false") {
      using_std_getline_ = false;
      return true;
    } else if (value == "true") {
      using_std_getline_ = true;
      return true;
    }
  }
  VLOG(1) << "error during configure local io adaptor with [" << key << ", "
          << value << "]";
  return false;
}

bool LocalIOAdaptor::SetPartialRead(const int index, const int total_parts) {
  //std::cout << "LocalIOAdaptor::SetPartialRead" << index << total_parts << std::endl;
  // make sure that the bytes of each line of the file
  // is smaller than macro FINELINE
  if (index < 0 || total_parts <= 0 || index >= total_parts) {
    VLOG(1) << "error during set_partial_read with [" << index << ", "
            << total_parts << "]";
    return false;
  }
  #ifdef WITH_HDFS
  if (fs_.is_open() || file_ != nullptr || hdfs_file_ != nullptr) {
    VLOG(2) << "WARNING!! std::set partial read after open have no effect,"
               "You probably want to set partial before open!";
    return false;
  }
  #else 
  if (fs_.is_open() || file_ != nullptr) {
    VLOG(2) << "WARNING!! std::set partial read after open have no effect,"
               "You probably want to set partial before open!";
    return false;
  }
  #endif
  enable_partial_read_ = true;
  index_ = index;
  total_parts_ = total_parts;
  return true;
}

bool LocalIOAdaptor::setPartialReadImpl() {
    //std::cout << "LocalIOAdaptor::setPartialReadImpl" << std::endl;
    seek(0, kFileLocationEnd);
    int64_t total_file_size = tell();
    int64_t part_size = total_file_size / total_parts_;

    partial_read_offset_.resize(total_parts_ + 1, 0);
    partial_read_offset_[total_parts_] = total_file_size;

    // move breakpoint to the next of nearest character '\n'
    for (int i = 1; i < total_parts_; ++i) {
      partial_read_offset_[i] = i * part_size;

      if (partial_read_offset_[i] < partial_read_offset_[i - 1]) {
        partial_read_offset_[i] = partial_read_offset_[i - 1];
      } else {
        // traversing backwards to find the nearest character '\n',
        seek(partial_read_offset_[i], kFileLocationBegin);
        int dis = 0;
        while (true) {
          char buffer[1];
          std::memset(buff, 0, sizeof(buffer));
          bool status = Read(buffer, 1);
          if (!status || buffer[0] == '\n') {
            break;
          } else {
            dis++;
          }
        }
        // move to next character of '\n'
        partial_read_offset_[i] += (dis + 1);
        if (partial_read_offset_[i] > total_file_size) {
          partial_read_offset_[i] = total_file_size;
        }
      }
    }

    int64_t file_stream_pos = partial_read_offset_[index_];
    seek(file_stream_pos, kFileLocationBegin);
    return true;
  
}

bool LocalIOAdaptor::ReadLine(std::string& line) {
  //std::cout << "LocalIOAdaptor::ReadLine " << std::endl;
  if (io_type_ == LOCAL) {
    if (enable_partial_read_ && tell() >= partial_read_offset_[index_ + 1]) {
      return false;
    }
    if (using_std_getline_) {
      getline(fs_, line);
      return !line.empty();
    } else {
      if (file_ && fgets(buff, LINE_SIZE, file_)) {
        std::string str(buff);
        line.swap(str);
        return true;
      } else {
        return false;
      }
    }
  } else {
    #ifdef WITH_HDFS
    // HDFS 专用读取行的实现
    return hdfsReadLine1(line);
    #else
    return false;
    #endif
  }
}

//sssp例子暂未用到
bool LocalIOAdaptor::ReadArchive(OutArchive& archive) {
  //std::cout << "LocalIOAdaptor::ReadArchive" << std::endl;
  if (!using_std_getline_ && file_) {
    size_t length;
    bool status = fread(&length, sizeof(size_t), 1, file_);
    if (!status) {
      return false;
    }
    archive.Allocate(length);
    status = fread(archive.GetBuffer(), 1, length, file_);
    return status;
  } else {
    VLOG(1) << "invalid operation.";
    return false;
  }
}

//sssp例子暂未用到
bool LocalIOAdaptor::WriteArchive(InArchive& archive) {
  //std::cout << "LocalIOAdaptor::WriteArchive" << std::endl;
  if (!using_std_getline_ && file_) {
    size_t length = archive.GetSize();
    bool status = fwrite(&length, sizeof(size_t), 1, file_);
    if (!status) {
      return false;
    }
    status = fwrite(archive.GetBuffer(), 1, length, file_);
    if (!status) {
      return false;
    }
    fflush(file_);
    return true;
  } else {
    VLOG(1) << "invalid operation.";
    return false;
  }
}

bool LocalIOAdaptor::Read(void* buffer, size_t size) {
  //std::cout << "LocalIOAdaptor::Read" << buffer << " " << size << std::endl;
  if (io_type_ == LOCAL) {
    if (using_std_getline_) {
      fs_.read(static_cast<char*>(buffer), size);
      if (!fs_) {
        return false;
      }
    } else {
      if (file_) {
        bool status = fread(buffer, 1, size, file_);
        if (!status) {
          return false;
        }
      } else {
        return false;
      }
    }
    return true;
  } else{
    #ifdef WITH_HDFS
    return hdfsRead1(buffer, size);
    #else
    return false;
    #endif
  }
  
}

//sssp例子暂未用到
bool LocalIOAdaptor::Write(void* buffer, size_t size) {
  //std::cout << "LocalIOAdaptor::Write" << buffer << " " << size << std::endl;
  if (using_std_getline_) {
    fs_.write(static_cast<char*>(buffer), size);
    if (!fs_) {
      return false;
    }
    fs_.flush();
  } else {
    if (file_) {
      bool status = fwrite(buffer, 1, size, file_);
      if (!status) {
        return false;
      }
      fflush(file_);
    } else {
      return false;
    }
  }
  return true;
}

void LocalIOAdaptor::Close() {
  //std::cout << "LocalIOAdaptor::Close" << std::endl;
  if (io_type_ == LOCAL) {
    if (using_std_getline_) {
      if (fs_.is_open()) {
        fs_.close();
      }
    } else {
      if (file_ != nullptr) {
        fclose(file_);
        file_ = nullptr;
      }
    }
  } else {
    #ifdef WITH_HDFS
    hdfsClose1();
    #endif
  }
  
}

//sssp例子暂未用到
void LocalIOAdaptor::MakeDirectory(const std::string& path) {
  //std::cout << "LocalIOAdaptor::MakeDirectory" << path << std::endl;
  std::string dir = path;
  int len = dir.size();
  if (dir[len - 1] != '/') {
    dir[len] = '/';
    len++;
  }
  std::string temp;
  for (int i = 1; i < len; i++) {
    if (dir[i] == '/') {
      temp = dir.substr(0, i);
      if (access(temp.c_str(), 0) != 0) {
        if (mkdir(temp.c_str(), 0777) != 0) {
          VLOG(1) << "failed operaiton.";
        }
      }
    }
  }
}

//sssp例子暂未用到
bool LocalIOAdaptor::IsExist() {
  //std::cout << "LocalIOAdaptor::IsExist" << std::endl; 
  return access(location_.c_str(), 0) == 0; 
}
#ifdef WITH_HDFS
// HDFS 专用 tell 实现
int64_t LocalIOAdaptor::hdfsTell1() {
  tOffset offset = hdfsTell(hdfs_conn_, hdfs_file_);
  if (offset == -1) {
    LOG(ERROR) << "Failed to get HDFS file position";
    return -1;
  }
  return static_cast<int64_t>(offset);
}

// HDFS 专用 seek 实现
void LocalIOAdaptor::hdfsSeek1(const int64_t offset, const FileLocation seek_from) {
  tOffset new_pos = -1;
  switch (seek_from) {
    case kFileLocationBegin:
      new_pos = offset;
      break;
    case kFileLocationCurrent:
      new_pos = hdfsTell(hdfs_conn_, hdfs_file_) + offset;
      break;
    case kFileLocationEnd:{
      hdfsFileInfo* info = hdfsGetPathInfo(hdfs_conn_, hdfs_file_path_.c_str());
      if (info) {
          tOffset size = info->mSize;
          hdfsFreeFileInfo(info, 1);  // 释放资源
          new_pos = size + offset;
      }
      break;
    }
    default:
      VLOG(1) << "invalid value, offset = " << offset
              << ", seek_from = " << seek_from;
      return;
  }
  
  if (hdfsSeek(hdfs_conn_, hdfs_file_, new_pos) != 0) {
    LOG(ERROR) << "Failed to seek to offset: " << new_pos 
               << ", whence: " << seek_from;
  }
}

// HDFS 专用 Open 实现
void LocalIOAdaptor::hdfsOpen1(const char* mode) {
  int hdfs_mode;
  if (strchr(mode, 'r')) {
    hdfs_mode = O_RDONLY;
  } else if (strchr(mode, 'w')) {
    hdfs_mode = O_WRONLY | O_CREAT | O_TRUNC;
  } else if (strchr(mode, 'a')) {
    hdfs_mode = O_WRONLY | O_CREAT | O_APPEND;
  } else {
    LOG(FATAL) << "Unsupported HDFS open mode: " << mode;
  }
  hdfs_file_ = hdfsOpenFile(hdfs_conn_, hdfs_file_path_.c_str(), hdfs_mode, 0, 0, 0);
  if (!hdfs_file_) {
    LOG(FATAL) << "Failed to open HDFS file: " << location_;
  }
}

// HDFS 专用 ReadLine 实现
bool LocalIOAdaptor::hdfsReadLine1(std::string& line) {
  //std::cout << "LocalIOAdaptor::ReadLine " << line << std::endl;
  line.clear();
  // 首次调用时初始化缓冲区
  if (buffer_size_ == 0 && buffer_pos_ == 0) {
    if (!hdfsFillBuffer()) {
        return false;  // 文件为空或读取失败
    }
  }
  // 从缓冲区读取数据

    // 查找换行符
    char* newline_pos = static_cast<char*>(memchr(
        buffer_.data() + buffer_pos_, 
        '\n', 
        buffer_size_ - buffer_pos_
    ));
    
    if (newline_pos != nullptr) {
        // 找到换行符，提取一行
        size_t line_length = newline_pos - (buffer_.data() + buffer_pos_) + 1;
        line.append(buffer_.data() + buffer_pos_, line_length);
        buffer_pos_ += line_length;
        return true;
    }
    
    // 缓冲区中没有完整的行
    if (buffer_pos_ < buffer_size_) {
        // 保存剩余内容到 line
        line.append(buffer_.data() + buffer_pos_, buffer_size_ - buffer_pos_);
        buffer_size_ = 0;
        buffer_pos_ = 0;
    }

    
    // 读取更多数据
    if (!hdfsFillBuffer()) {
        // 没有更多数据，返回已读取的内容（可能为空行）
        return !line.empty();
    } else {
      // 检查新读取的数据中是否有换行符
      newline_pos = static_cast<char*>(memchr(
          buffer_.data(), 
          '\n', 
          buffer_size_
      ));
      
      if (newline_pos != nullptr) {
          // 找到换行符，提取剩余行内容
          size_t line_length = newline_pos - buffer_.data() + 1;
          line.append(buffer_.data(), line_length);
          buffer_pos_ += line_length;
          return true;
      }
    }
    return false;
}

// HDFS填充缓冲区
bool LocalIOAdaptor::hdfsFillBuffer() {
  // 确保缓冲区已分配足够空间
  if (buffer_.size() < HDFS_LINE_SIZE) {
    buffer_.resize(HDFS_LINE_SIZE);
  }
  int64_t bytes_to_read = HDFS_LINE_SIZE;
  int64_t total_bytes_read = 0;
  
  // 如果启用了分区读取，计算本次最多能读取的字节数，确保不超出分区边界
  if (enable_partial_read_) {
      int64_t current_pos = tell();
      int64_t bytes_remaining = partial_read_offset_[index_ + 1] - current_pos;
      if (bytes_remaining < bytes_to_read) {
          bytes_to_read = bytes_remaining;
      }
  }
  if (bytes_to_read <= 0) {
      return false;
  }
  
  // 循环读取数据，直到填满缓冲区或达到分区边界,有时候一次读取不会成功，所以需要循环读取（太坑）
  while (total_bytes_read < bytes_to_read) {
    tSize bytesRead = hdfsRead(
        hdfs_conn_, 
        hdfs_file_, 
        buffer_.data() + total_bytes_read,  // 写入位置偏移
        bytes_to_read - total_bytes_read  // 剩余需要读取的字节数
    );
    
    if (bytesRead <= 0) {
        break;  // 读取失败或已到达文件末尾
    }
    
    total_bytes_read += bytesRead;
  } 
  
  if (total_bytes_read > 0) {
    buffer_size_ = total_bytes_read;
    buffer_pos_ = 0;
    return true;
  }

  return false;
}

// HDFS 专用 Read 实现
bool LocalIOAdaptor::hdfsRead1(void* buffer, size_t size) {
  tSize bytesRead = hdfsRead(hdfs_conn_, hdfs_file_, buffer, size);
  if(bytesRead < 0) {
    LOG(ERROR) << "Failed to read from HDFS file: " << location_;
    return false;
  }
  return true;
}

// HDFS 专用 Close 实现
bool LocalIOAdaptor::hdfsClose1() {
  if (hdfs_file_) {
    hdfsCloseFile(hdfs_conn_, hdfs_file_);
    hdfs_file_ = nullptr;
    return true;
  }
  return false;
}

#endif
}  // namespace grape
