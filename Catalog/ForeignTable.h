/*
 * Copyright 2020 OmniSci, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "ForeignServer.h"

#include <boost/algorithm/string/predicate.hpp>
#include "OptionsContainer.h"
#include "Shared/DateTimeParser.h"
#include "TableDescriptor.h"

namespace foreign_storage {
struct ForeignTable : public TableDescriptor, public OptionsContainer {
  static constexpr const char* FRAGMENT_SIZE_KEY = "FRAGMENT_SIZE";
  static constexpr const char* REFRESH_TIMING_TYPE_KEY = "REFRESH_TIMING_TYPE";
  static constexpr const char* REFRESH_START_DATE_TIME_KEY = "REFRESH_START_DATE_TIME";
  static constexpr const char* REFRESH_INTERVAL_KEY = "REFRESH_INTERVAL";
  static constexpr const char* REFRESH_UPDATE_TYPE_KEY = "REFRESH_UPDATE_TYPE";
  static constexpr const char* ALL_REFRESH_UPDATE_TYPE = "ALL";
  static constexpr const char* APPEND_REFRESH_UPDATE_TYPE = "APPEND";
  static constexpr const char* SCHEDULE_REFRESH_TIMING_TYPE = "SCHEDULED";
  static constexpr const char* MANUAL_REFRESH_TIMING_TYPE = "MANUAL";
  static constexpr int NULL_REFRESH_TIME = -1;

  const ForeignServer* foreign_server;
  int64_t last_refresh_time{NULL_REFRESH_TIME}, next_refresh_time{NULL_REFRESH_TIME};
  static constexpr std::array<const char*, 5> supported_options{
      FRAGMENT_SIZE_KEY,
      REFRESH_TIMING_TYPE_KEY,
      REFRESH_START_DATE_TIME_KEY,
      REFRESH_INTERVAL_KEY,
      REFRESH_UPDATE_TYPE_KEY};

  void validate(const std::vector<std::string_view>& supported_data_wrapper_options) {
    auto update_type_entry =
        options.find(foreign_storage::ForeignTable::REFRESH_UPDATE_TYPE_KEY);
    CHECK(update_type_entry != options.end());
    if (update_type_entry->second != ALL_REFRESH_UPDATE_TYPE &&
        update_type_entry->second != APPEND_REFRESH_UPDATE_TYPE) {
      std::string error_message = "Invalid value \"" + update_type_entry->second +
                                  "\" for " + REFRESH_UPDATE_TYPE_KEY + " option." +
                                  " Value must be \"" +
                                  std::string{APPEND_REFRESH_UPDATE_TYPE} + "\" or \"" +
                                  std::string{ALL_REFRESH_UPDATE_TYPE} + "\".";
      throw std::runtime_error{error_message};
    }

    auto refresh_timing_entry =
        options.find(foreign_storage::ForeignTable::REFRESH_TIMING_TYPE_KEY);
    CHECK(refresh_timing_entry != options.end());
    if (refresh_timing_entry->second == SCHEDULE_REFRESH_TIMING_TYPE) {
      auto start_date_entry = options.find(REFRESH_START_DATE_TIME_KEY);
      if (start_date_entry == options.end()) {
        throw std::runtime_error{std::string{REFRESH_START_DATE_TIME_KEY} +
                                 " option must be provided for scheduled refreshes."};
      }
      auto start_date_time = dateTimeParse<kTIMESTAMP>(start_date_entry->second, 0);
      int64_t current_time = std::chrono::duration_cast<std::chrono::seconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
      if (start_date_time < current_time) {
        throw std::runtime_error{std::string{REFRESH_START_DATE_TIME_KEY} +
                                 " cannot be a past date time."};
      }

      auto interval_entry = options.find(REFRESH_INTERVAL_KEY);
      if (interval_entry != options.end()) {
        boost::regex interval_regex{"^\\d{1,}[SHD]$",
                                    boost::regex::extended | boost::regex::icase};
        if (!boost::regex_match(interval_entry->second, interval_regex)) {
          throw std::runtime_error{"Invalid value provided for the " +
                                   std::string{REFRESH_INTERVAL_KEY} + " option."};
        }
      }
    } else if (refresh_timing_entry->second != MANUAL_REFRESH_TIMING_TYPE) {
      throw std::runtime_error{"Invalid value provided for the " +
                               std::string{REFRESH_TIMING_TYPE_KEY} +
                               " option. Value must be \"" + MANUAL_REFRESH_TIMING_TYPE +
                               "\" or \"" + SCHEDULE_REFRESH_TIMING_TYPE + "\"."};
    }
    validateRecognizedOption(supported_data_wrapper_options);
  }

  bool isAppendMode() const {
    auto update_mode = options.find(REFRESH_UPDATE_TYPE_KEY);
    return (update_mode != options.end() &&
            update_mode->second == APPEND_REFRESH_UPDATE_TYPE);
  }

 private:
  void validateRecognizedOption(
      const std::vector<std::string_view>& supported_data_wrapper_options) {
    for (const auto& entry : options) {
      if (std::find(supported_options.begin(), supported_options.end(), entry.first) ==
              supported_options.end() &&
          std::find(supported_data_wrapper_options.begin(),
                    supported_data_wrapper_options.end(),
                    entry.first) == supported_data_wrapper_options.end()) {
        std::string error_message =
            "Invalid foreign table option \"" + entry.first + "\".";
        throw std::runtime_error{error_message};
      }
    }
  }
};
}  // namespace foreign_storage
