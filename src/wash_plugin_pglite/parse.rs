use crate::interlude::*;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::{DataRowBody, Message};
use postgres_protocol::types as pg_types;
use postgres_protocol::Oid;
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};
use uuid::Uuid;

use crate::plugin::types;

#[derive(Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub type_oid: Oid,
}

pub fn parse_wire_response(data: &[u8]) -> Vec<types::ResultRow> {
    if data.is_empty() {
        warn!("parse_wire_response: received empty data");
        return Vec::new();
    }
    
    debug!("parse_wire_response: parsing {} bytes", data.len());
    let mut buf = BytesMut::from(data);
    let mut columns: Vec<ColumnInfo> = Vec::new();
    let mut rows = Vec::new();
    let mut message_count = 0;

    loop {
        match Message::parse(&mut buf) {
            Ok(Some(Message::RowDescription(body))) => {
                debug!("Received RowDescription");
                columns.clear();
                let mut fields = body.fields();
                while let Ok(Some(field)) = fields.next() {
                    columns.push(ColumnInfo {
                        name: field.name().to_string(),
                        type_oid: field.type_oid(),
                    });
                }
                debug!("RowDescription: {} columns", columns.len());
            }
            Ok(Some(Message::DataRow(body))) => {
                message_count += 1;
                if columns.is_empty() {
                    // Try to continue without columns - this shouldn't happen but let's be defensive
                    warn!("DataRow received before RowDescription (message #{})", message_count);
                    // Don't panic, just skip this row
                    continue;
                }
                if let Some(row) = parse_data_row(&body, &columns) {
                    rows.push(row);
                }
            }
            Ok(Some(Message::ReadyForQuery(_))) => {
                debug!("Received ReadyForQuery, parsed {} rows", rows.len());
                break;
            }
            Ok(Some(msg)) => {
                match msg {
                    Message::CommandComplete(_) => {
                        debug!("Received CommandComplete");
                    }
                    Message::ParameterStatus(_) | 
                    Message::NoticeResponse(_) | Message::ParseComplete | 
                    Message::BindComplete | Message::ParameterDescription(_) => {
                        // Expected messages, continue
                    }
                    other => {
                        debug!("Received unexpected message type: {:?}", std::any::type_name_of_val(&other));
                        // Don't panic on unexpected messages, just log and continue
                    }
                }
            },
            Ok(None) => {
                debug!("No more messages to parse");
                break;
            },
            Err(e) => {
                debug!("Error parsing message: {:?}", e);
                break;
            },
        }
    }

    debug!("parse_wire_response returning {} rows ({} messages processed)", rows.len(), message_count);
    rows
}

fn parse_data_row(body: &DataRowBody, columns: &[ColumnInfo]) -> Option<types::ResultRow> {
    let mut row = Vec::new();
    let mut ranges = body.ranges();
    let buffer = body.buffer();
    let mut i = 0;

    while let Ok(Some(range_opt)) = ranges.next() {
        let column_info = columns.get(i).cloned().unwrap_or_else(|| ColumnInfo {
            name: format!("col{}", i),
            type_oid: 0,
        });

        let value = match range_opt {
            Some(range) => {
                let value_bytes = &buffer[range];
                parse_value_by_oid(value_bytes, column_info.type_oid)
            }
            None => types::PgValue::Null,
        };

        row.push(types::ResultRowEntry {
            column_name: column_info.name,
            value,
        });
        i += 1;
    }

    Some(row)
}

fn parse_value_by_oid(bytes: &[u8], oid: Oid) -> types::PgValue {
    match oid {
        16 => {
            // PostgreSQL returns 't' for true and 'f' for false in text format
            // Parse text format explicitly to avoid binary format issues
            if let Ok(s) = std::str::from_utf8(bytes) {
                let trimmed = s.trim();
                match trimmed {
                    "t" | "true" | "TRUE" | "1" => types::PgValue::Bool(true),
                    "f" | "false" | "FALSE" | "0" => types::PgValue::Bool(false),
                    _ => {
                        // Try binary format as fallback
                        if let Ok(b) = pg_types::bool_from_sql(bytes) {
                            types::PgValue::Bool(b)
                        } else {
                            types::PgValue::Text(s.to_string())
                        }
                    }
                }
            } else if bytes.len() == 1 {
                // Single byte check (most common case: 't' or 'f')
                match bytes[0] {
                    b't' => types::PgValue::Bool(true),
                    b'f' => types::PgValue::Bool(false),
                    _ => {
                        // Try binary format
                        if let Ok(b) = pg_types::bool_from_sql(bytes) {
                            types::PgValue::Bool(b)
                        } else {
                            types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                        }
                    }
                }
            } else {
                // Try binary format first for non-text, non-single-byte
                if let Ok(b) = pg_types::bool_from_sql(bytes) {
                    types::PgValue::Bool(b)
                } else {
                    types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                }
            }
        }
        21 => {
            if let Ok(i) = pg_types::int2_from_sql(bytes) {
                types::PgValue::Int2(i)
            } else if let Ok(s) = std::str::from_utf8(bytes) {
                if let Ok(i) = s.parse::<i16>() {
                    types::PgValue::Int2(i)
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        23 => match pg_types::int4_from_sql(bytes) {
            Ok(i) => types::PgValue::Int4(i),
            Err(_) => {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    if let Ok(i) = s.parse::<i32>() {
                        types::PgValue::Int4(i)
                    } else {
                        types::PgValue::Text(s.to_string())
                    }
                } else {
                    types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                }
            }
        },
        20 => {
            if let Ok(i) = pg_types::int8_from_sql(bytes) {
                types::PgValue::Int8(i)
            } else if let Ok(s) = std::str::from_utf8(bytes) {
                if let Ok(i) = s.parse::<i64>() {
                    types::PgValue::Int8(i)
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        25 | 1043 | 1042 | 19 => {
            if let Ok(s) = pg_types::text_from_sql(bytes) {
                types::PgValue::Text(s.to_string())
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        17 => {
            let decoded = pg_types::bytea_from_sql(bytes);
            types::PgValue::Bytea(decoded.to_vec())
        }
        700 => {
            if let Ok(f) = pg_types::float4_from_sql(bytes) {
                let bits = f.to_bits();
                types::PgValue::Float4((bits as u64, 0i16, 0i8))
            } else if let Ok(s) = std::str::from_utf8(bytes) {
                if let Ok(f) = s.parse::<f32>() {
                    let bits = f.to_bits();
                    types::PgValue::Float4((bits as u64, 0i16, 0i8))
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        701 => {
            if let Ok(f) = pg_types::float8_from_sql(bytes) {
                let bits = f.to_bits();
                types::PgValue::Float8((bits, 0i16, 0i8))
            } else if let Ok(s) = std::str::from_utf8(bytes) {
                if let Ok(f) = s.parse::<f64>() {
                    let bits = f.to_bits();
                    types::PgValue::Float8((bits, 0i16, 0i8))
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        1082 => match pg_types::date_from_sql(bytes) {
            Ok(days) => types::PgValue::Date(pg_date_to_wit_date(days)),
            Err(_) => {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    if let Ok(date) =
                        Date::parse(s, &time::format_description::well_known::Iso8601::DATE)
                    {
                        types::PgValue::Date(types::Date::Ymd((
                            date.year(),
                            date.month() as u32,
                            date.day() as u32,
                        )))
                    } else {
                        types::PgValue::Text(s.to_string())
                    }
                } else {
                    types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                }
            }
        },
        1083 => {
            // Parse time from text format first (more reliable with pglite)
            if let Ok(s) = std::str::from_utf8(bytes) {
                if let Ok(time) =
                    Time::parse(s, &time::format_description::well_known::Iso8601::TIME)
                {
                    types::PgValue::Time(types::Time {
                        hour: time.hour() as u32,
                        min: time.minute() as u32,
                        sec: time.second() as u32,
                        micro: time.nanosecond() / 1000,
                    })
                } else {
                    // Fallback to binary format
                    match pg_types::time_from_sql(bytes) {
                        Ok(microseconds) => types::PgValue::Time(pg_time_to_wit_time(microseconds)),
                        Err(_) => types::PgValue::Text(s.to_string()),
                    }
                }
            } else {
                // Try binary format
                match pg_types::time_from_sql(bytes) {
                    Ok(microseconds) => types::PgValue::Time(pg_time_to_wit_time(microseconds)),
                    Err(_) => types::PgValue::Text(String::from_utf8_lossy(bytes).to_string()),
                }
            }
        },
        1114 => {
            // Parse timestamp from text format first (more reliable with pglite)
            if let Ok(s) = std::str::from_utf8(bytes) {
                // Try PostgreSQL format (space separator) first, then ISO8601
                // Replace space with 'T' to convert to ISO8601 format
                let iso_str = s.replace(' ', "T");
                let datetime_result = PrimitiveDateTime::parse(&iso_str, &time::format_description::well_known::Iso8601::DATE_TIME)
                    .or_else(|_| PrimitiveDateTime::parse(s, &time::format_description::well_known::Iso8601::DATE_TIME));
                
                if let Ok(datetime) = datetime_result {
                    types::PgValue::Timestamp(types::Timestamp {
                        date: types::Date::Ymd((
                            datetime.date().year(),
                            datetime.date().month() as u32,
                            datetime.date().day() as u32,
                        )),
                        time: types::Time {
                            hour: datetime.time().hour() as u32,
                            min: datetime.time().minute() as u32,
                            sec: datetime.time().second() as u32,
                            micro: datetime.time().nanosecond() / 1000,
                        },
                    })
                } else {
                    // Fallback to binary format
                    match pg_types::timestamp_from_sql(bytes) {
                        Ok(microseconds) => {
                            let base_datetime = PrimitiveDateTime::new(
                                Date::from_calendar_date(2000, time::Month::January, 1)
                                    .expect("invalid base date"),
                                Time::MIDNIGHT,
                            );
                            let datetime = base_datetime + time::Duration::microseconds(microseconds);
                            types::PgValue::Timestamp(types::Timestamp {
                                date: types::Date::Ymd((
                                    datetime.date().year(),
                                    datetime.date().month() as u32,
                                    datetime.date().day() as u32,
                                )),
                                time: types::Time {
                                    hour: datetime.time().hour() as u32,
                                    min: datetime.time().minute() as u32,
                                    sec: datetime.time().second() as u32,
                                    micro: datetime.time().nanosecond() / 1000,
                                },
                            })
                        }
                        Err(_) => types::PgValue::Text(s.to_string()),
                    }
                }
            } else {
                // Try binary format
                match pg_types::timestamp_from_sql(bytes) {
                    Ok(microseconds) => {
                        let base_datetime = PrimitiveDateTime::new(
                            Date::from_calendar_date(2000, time::Month::January, 1)
                                .expect("invalid base date"),
                            Time::MIDNIGHT,
                        );
                        let datetime = base_datetime + time::Duration::microseconds(microseconds);
                        types::PgValue::Timestamp(types::Timestamp {
                            date: types::Date::Ymd((
                                datetime.date().year(),
                                datetime.date().month() as u32,
                                datetime.date().day() as u32,
                            )),
                            time: types::Time {
                                hour: datetime.time().hour() as u32,
                                min: datetime.time().minute() as u32,
                                sec: datetime.time().second() as u32,
                                micro: datetime.time().nanosecond() / 1000,
                            },
                        })
                    }
                    Err(_) => types::PgValue::Text(String::from_utf8_lossy(bytes).to_string()),
                }
            }
        }
        1184 => {
            // Parse timestamptz from text format first (more reliable with pglite)
            if let Ok(s) = std::str::from_utf8(bytes) {
                // Try parsing with timezone offset
                // Replace space with 'T' and try ISO8601 format
                let iso_str = s.replace(' ', "T");
                if let Ok(datetime) = OffsetDateTime::parse(&iso_str, &time::format_description::well_known::Iso8601::DATE_TIME) {
                    let offset_secs = datetime.offset().whole_seconds();
                    // PostgreSQL uses WesternHemisphereSecs for positive offsets (east of UTC)
                    // and EasternHemisphereSecs for negative offsets (west of UTC)
                    // This is opposite of what you might expect
                    let offset = if offset_secs >= 0 {
                        types::Offset::WesternHemisphereSecs(offset_secs)
                    } else {
                        types::Offset::EasternHemisphereSecs(-offset_secs)
                    };
                    types::PgValue::TimestampTz(types::TimestampTz {
                        timestamp: types::Timestamp {
                            date: types::Date::Ymd((
                                datetime.date().year(),
                                datetime.date().month() as u32,
                                datetime.date().day() as u32,
                            )),
                            time: types::Time {
                                hour: datetime.time().hour() as u32,
                                min: datetime.time().minute() as u32,
                                sec: datetime.time().second() as u32,
                                micro: datetime.time().nanosecond() / 1000,
                            },
                        },
                        offset,
                    })
                } else {
                    // Fallback to binary format
                    if let Ok(microseconds) = pg_types::timestamp_from_sql(bytes) {
                        let base_datetime = OffsetDateTime::new_utc(
                            Date::from_calendar_date(2000, time::Month::January, 1)
                                .expect("invalid base date"),
                            Time::MIDNIGHT,
                        );
                        let datetime = base_datetime + time::Duration::microseconds(microseconds);
                        let offset_secs = datetime.offset().whole_seconds();
                        let offset = if offset_secs >= 0 {
                            types::Offset::EasternHemisphereSecs(offset_secs)
                        } else {
                            types::Offset::WesternHemisphereSecs(-offset_secs)
                        };
                        types::PgValue::TimestampTz(types::TimestampTz {
                            timestamp: types::Timestamp {
                                date: types::Date::Ymd((
                                    datetime.date().year(),
                                    datetime.date().month() as u32,
                                    datetime.date().day() as u32,
                                )),
                                time: types::Time {
                                    hour: datetime.time().hour() as u32,
                                    min: datetime.time().minute() as u32,
                                    sec: datetime.time().second() as u32,
                                    micro: datetime.time().nanosecond() / 1000,
                                },
                            },
                            offset,
                        })
                    } else {
                        types::PgValue::Text(s.to_string())
                    }
                }
            } else {
                // Try binary format
                if let Ok(microseconds) = pg_types::timestamp_from_sql(bytes) {
                    let base_datetime = OffsetDateTime::new_utc(
                        Date::from_calendar_date(2000, time::Month::January, 1)
                            .expect("invalid base date"),
                        Time::MIDNIGHT,
                    );
                    let datetime = base_datetime + time::Duration::microseconds(microseconds);
                    let offset_secs = datetime.offset().whole_seconds();
                    // PostgreSQL uses WesternHemisphereSecs for positive offsets (east of UTC)
                    // and EasternHemisphereSecs for negative offsets (west of UTC)
                    // This is opposite of what you might expect
                    let offset = if offset_secs >= 0 {
                        types::Offset::WesternHemisphereSecs(offset_secs)
                    } else {
                        types::Offset::EasternHemisphereSecs(-offset_secs)
                    };
                    types::PgValue::TimestampTz(types::TimestampTz {
                        timestamp: types::Timestamp {
                            date: types::Date::Ymd((
                                datetime.date().year(),
                                datetime.date().month() as u32,
                                datetime.date().day() as u32,
                            )),
                            time: types::Time {
                                hour: datetime.time().hour() as u32,
                                min: datetime.time().minute() as u32,
                                sec: datetime.time().second() as u32,
                                micro: datetime.time().nanosecond() / 1000,
                            },
                        },
                        offset,
                    })
                } else {
                    types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                }
            }
        }
        1266 => {
            // TimeTz - similar to TimestampTz but for time with timezone
            if let Ok(s) = std::str::from_utf8(bytes) {
                if let Ok(datetime) = OffsetDateTime::parse(
                    s,
                    &time::format_description::well_known::Iso8601::DATE_TIME,
                ) {
                    let offset_secs = datetime.offset().whole_seconds();
                    // PostgreSQL uses WesternHemisphereSecs for positive offsets (east of UTC)
                    // and EasternHemisphereSecs for negative offsets (west of UTC)
                    // This is opposite of what you might expect
                    let offset = if offset_secs >= 0 {
                        types::Offset::WesternHemisphereSecs(offset_secs)
                    } else {
                        types::Offset::EasternHemisphereSecs(-offset_secs)
                    };
                    types::PgValue::TimestampTz(types::TimestampTz {
                        timestamp: types::Timestamp {
                            date: types::Date::Ymd((
                                datetime.date().year(),
                                datetime.date().month() as u32,
                                datetime.date().day() as u32,
                            )),
                            time: types::Time {
                                hour: datetime.time().hour() as u32,
                                min: datetime.time().minute() as u32,
                                sec: datetime.time().second() as u32,
                                micro: datetime.time().nanosecond() / 1000,
                            },
                        },
                        offset,
                    })
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        1186 => {
            // Interval - parse as text string since postgres_protocol doesn't have interval parsing
            // and our WIT type requires stringification
            if let Ok(s) = std::str::from_utf8(bytes) {
                // Try to parse interval string format: "1 day 2 hours 3 minutes"
                // For now, store as text since interval parsing is complex
                types::PgValue::Text(s.to_string())
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        1266 => {
            // TimeTz - parse using time_from_sql and extract timezone
            match pg_types::time_from_sql(bytes) {
                Ok(microseconds) => {
                    // Parse timezone from text representation since postgres_protocol doesn't provide it
                    if let Ok(s) = std::str::from_utf8(bytes) {
                        // Extract timezone from string like "12:34:56+05:30"
                        let time_part = pg_time_to_wit_time(microseconds);
                        let tz_str = if let Some(plus_idx) = s.rfind('+') {
                            s[plus_idx..].to_string()
                        } else if let Some(minus_idx) = s.rfind('-') {
                            // Check if it's a timezone offset (not just a time separator)
                            if minus_idx > 2 && s.chars().nth(minus_idx - 1).map_or(false, |c| c.is_ascii_digit()) {
                                s[minus_idx..].to_string()
                            } else {
                                "+00:00".to_string()
                            }
                        } else {
                            "+00:00".to_string()
                        };
                        types::PgValue::TimeTz(types::TimeTz {
                            timesonze: tz_str,
                            time: time_part,
                        })
                    } else {
                        types::PgValue::TimeTz(types::TimeTz {
                            timesonze: "+00:00".to_string(),
                            time: pg_time_to_wit_time(microseconds),
                        })
                    }
                }
                Err(_) => {
                    if let Ok(s) = std::str::from_utf8(bytes) {
                        types::PgValue::Text(s.to_string())
                    } else {
                        types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
                    }
                }
            }
        }
        869 => {
            // INET
            if let Ok(inet) = pg_types::inet_from_sql(bytes) {
                types::PgValue::Inet(inet.addr().to_string())
            } else if let Ok(s) = std::str::from_utf8(bytes) {
                types::PgValue::Inet(s.to_string())
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        650 => {
            // CIDR - similar to INET but with netmask
            if let Ok(inet) = pg_types::inet_from_sql(bytes) {
                let addr_str = inet.addr().to_string();
                let netmask = inet.netmask();
                types::PgValue::Cidr(format!("{}/{}", addr_str, netmask))
            } else if let Ok(s) = std::str::from_utf8(bytes) {
                types::PgValue::Cidr(s.to_string())
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        829 => {
            // MACADDR
            if let Ok(mac_bytes) = pg_types::macaddr_from_sql(bytes) {
                types::PgValue::Macaddr(types::MacAddressEui48 {
                    bytes: (mac_bytes[0], mac_bytes[1], mac_bytes[2], mac_bytes[3], mac_bytes[4], mac_bytes[5]),
                })
            } else if let Ok(s) = std::str::from_utf8(bytes) {
                // Try to parse MAC address string format
                if let Some(mac) = parse_macaddr_string(s) {
                    types::PgValue::Macaddr(mac)
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        600 => {
            // POINT
            if let Ok(point) = pg_types::point_from_sql(bytes) {
                let x = point.x();
                let y = point.y();
                types::PgValue::Point(((x.to_bits(), 0i16, 0i8), (y.to_bits(), 0i16, 0i8)))
            } else if let Ok(s) = std::str::from_utf8(bytes) {
                // Try to parse point string format: "(1.5, 2.5)"
                if let Some(point) = parse_point_string(s) {
                    types::PgValue::Point(point)
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        603 => {
            // BOX
            if let Ok(box_val) = pg_types::box_from_sql(bytes) {
                let ll = box_val.lower_left();
                let ur = box_val.upper_right();
                types::PgValue::Box((
                    ((ll.x().to_bits(), 0i16, 0i8), (ll.y().to_bits(), 0i16, 0i8)),
                    ((ur.x().to_bits(), 0i16, 0i8), (ur.y().to_bits(), 0i16, 0i8)),
                ))
            } else if let Ok(s) = std::str::from_utf8(bytes) {
                // Try to parse box string format: "((0,0),(1,1))"
                if let Some(box_val) = parse_box_string(s) {
                    types::PgValue::Box(box_val)
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        718 => {
            // CIRCLE - parse from string format: "((x,y),r)"
            if let Ok(s) = std::str::from_utf8(bytes) {
                if let Some(circle) = parse_circle_string(s) {
                    types::PgValue::Circle(circle)
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        602 => {
            // PATH
            if let Ok(path) = pg_types::path_from_sql(bytes) {
                let mut points = Vec::new();
                let mut path_points = path.points();
                while let Ok(Some(point)) = path_points.next() {
                    points.push(((point.x().to_bits(), 0i16, 0i8), (point.y().to_bits(), 0i16, 0i8)));
                }
                types::PgValue::Path(points)
            } else if let Ok(s) = std::str::from_utf8(bytes) {
                // Try to parse path string format: "[(0,0),(1,1)]"
                if let Some(path) = parse_path_string(s) {
                    types::PgValue::Path(path)
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        628 => {
            // LINE
            if let Ok(s) = std::str::from_utf8(bytes) {
                // Line format: "{A,B,C}" representing Ax + By + C = 0
                if let Some(line) = parse_line_string(s) {
                    types::PgValue::Line(line)
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        601 => {
            // LSEG
            if let Ok(s) = std::str::from_utf8(bytes) {
                // Lseg format: "[(x1,y1),(x2,y2)]"
                if let Some(lseg) = parse_lseg_string(s) {
                    types::PgValue::Lseg(lseg)
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        604 => {
            // POLYGON
            if let Ok(s) = std::str::from_utf8(bytes) {
                // Polygon format: "((x1,y1),(x2,y2),...)"
                if let Some(polygon) = parse_polygon_string(s) {
                    types::PgValue::Polygon(polygon)
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        1700 => {
            // NUMERIC/DECIMAL - stored as string in WIT
            if let Ok(s) = std::str::from_utf8(bytes) {
                types::PgValue::Numeric(s.to_string())
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        1560 | 1562 => {
            // BIT/VARBIT
            if let Ok(varbit) = pg_types::varbit_from_sql(bytes) {
                let len = varbit.len();
                let bytes_vec = varbit.bytes().to_vec();
                if oid == 1560 {
                    // BIT - fixed length
                    types::PgValue::Bit((len as u32, bytes_vec))
                } else {
                    // VARBIT - variable length
                    types::PgValue::Varbit((Some(len as u32), bytes_vec))
                }
            } else if let Ok(s) = std::str::from_utf8(bytes) {
                // Try to parse bit string format: "B'1010'"
                if let Some(bit_val) = parse_bit_string(s, oid == 1560) {
                    types::PgValue::Bit(bit_val)
                } else if let Some(varbit_val) = parse_varbit_string(s) {
                    types::PgValue::Varbit(varbit_val)
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        3220 => {
            // PG_LSN
            if let Ok(lsn) = pg_types::lsn_from_sql(bytes) {
                types::PgValue::PgLsn(lsn)
            } else if let Ok(s) = std::str::from_utf8(bytes) {
                // Try to parse LSN string format: "0/12345678"
                if let Some(lsn) = parse_pg_lsn_string(s) {
                    types::PgValue::PgLsn(lsn)
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        142 => {
            // XML - stored as string
            if let Ok(s) = std::str::from_utf8(bytes) {
                types::PgValue::Xml(s.to_string())
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        790 => {
            // MONEY - stored as string (numeric in WIT)
            if let Ok(s) = std::str::from_utf8(bytes) {
                types::PgValue::Money(s.to_string())
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        18 => {
            // CHAR (single byte)
            if let Ok(char_val) = pg_types::char_from_sql(bytes) {
                types::PgValue::Char((1u32, vec![char_val as u8]))
            } else if let Ok(s) = std::str::from_utf8(bytes) {
                if s.len() == 1 {
                    types::PgValue::Char((1u32, vec![s.as_bytes()[0]]))
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        3615 => {
            // TSQUERY - stored as string
            if let Ok(s) = std::str::from_utf8(bytes) {
                types::PgValue::TsQuery(s.to_string())
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        3614 => {
            // TSVECTOR - parse lexemes
            if let Ok(s) = std::str::from_utf8(bytes) {
                // Parse tsvector format: "'word':1A 'another':2B"
                if let Some(tsvector) = parse_tsvector_string(s) {
                    types::PgValue::TsVector(tsvector)
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        2950 => {
            if let Ok(uuid_bytes) = pg_types::uuid_from_sql(bytes) {
                let uuid = Uuid::from_bytes(uuid_bytes);
                types::PgValue::Uuid(uuid.to_string())
            } else if let Ok(s) = std::str::from_utf8(bytes) {
                if Uuid::parse_str(s).is_ok() {
                    types::PgValue::Uuid(s.to_string())
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        114 => {
            if let Ok(s) = std::str::from_utf8(bytes) {
                if serde_json::from_str::<serde_json::Value>(s).is_ok() {
                    types::PgValue::Json(s.to_string())
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        3802 => {
            if let Ok(s) = std::str::from_utf8(bytes) {
                if serde_json::from_str::<serde_json::Value>(s).is_ok() {
                    types::PgValue::Jsonb(s.to_string())
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        // HSTORE - extension type, OID varies but commonly around 16392-16395
        // Try to detect and parse hstore
        oid if oid >= 16390 && oid <= 16400 => {
            if let Ok(hstore_entries) = pg_types::hstore_from_sql(bytes) {
                let mut entries = Vec::new();
                let mut iter = hstore_entries;
                while let Ok(Some((key, value))) = iter.next() {
                    entries.push((key.to_string(), value.map(|v| v.to_string())));
                }
                types::PgValue::Hstore(entries)
            } else if let Ok(s) = std::str::from_utf8(bytes) {
                // Try to parse hstore string format: "a=>1, b=>2"
                if let Some(hstore) = parse_hstore_string(s) {
                    types::PgValue::Hstore(hstore)
                } else {
                    types::PgValue::Text(s.to_string())
                }
            } else {
                types::PgValue::Text(String::from_utf8_lossy(bytes).to_string())
            }
        }
        // Array types - PostgreSQL array OIDs are typically base_type_oid + some offset
        // Common array OIDs: 1000 (bool[]), 1005 (int2[]), 1007 (int4[]), 1016 (int8[]),
        // 1021 (float4[]), 1022 (float8[]), 1009 (text[]), 1001 (bytea[]), 2951 (uuid[])
        // Try to parse as array first for any OID
        _oid => {
            // Try to parse as array first - array_from_sql will fail if it's not an array
            if let Ok(array) = pg_types::array_from_sql(bytes) {
                if let Some(array_value) = parse_array_value(&array) {
                    return array_value;
                }
            }
            
            // Not an array, try other parsing
            // Check for common scalar types that might not have been handled
            if let Ok(s) = std::str::from_utf8(bytes) {
                // Try numeric parsing first
                if let Ok(i) = s.parse::<i32>() {
                    types::PgValue::Int4(i)
                } else if let Ok(i) = s.parse::<i64>() {
                    types::PgValue::Int8(i)
                } else if let Ok(f) = s.parse::<f64>() {
                    // Could be a float, but we don't know if it's float4 or float8
                    // Default to float8
                    types::PgValue::Float8((f.to_bits(), 0i16, 0i8))
                } else {
                    // Default to text
                    types::PgValue::Text(s.to_string())
                }
            } else {
                // Binary data - default to bytea
                types::PgValue::Bytea(bytes.to_vec())
            }
        }
    }
}

// Helper functions for parsing string representations
fn parse_macaddr_string(s: &str) -> Option<types::MacAddressEui48> {
    // Parse formats like "08:00:2b:01:02:03" or "08-00-2b-01-02-03"
    let parts: Vec<&str> = s.split(|c| c == ':' || c == '-').collect();
    if parts.len() == 6 {
        let mut bytes = [0u8; 6];
        for (i, part) in parts.iter().enumerate() {
            if let Ok(b) = u8::from_str_radix(part.trim(), 16) {
                bytes[i] = b;
            } else {
                return None;
            }
        }
        Some(types::MacAddressEui48 {
            bytes: (bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]),
        })
    } else {
        None
    }
}

fn parse_point_string(s: &str) -> Option<((u64, i16, i8), (u64, i16, i8))> {
    // Parse format: "(x, y)" or "(x,y)"
    let s = s.trim();
    if s.starts_with('(') && s.ends_with(')') {
        let coords = s[1..s.len() - 1].split(',').collect::<Vec<_>>();
        if coords.len() == 2 {
            if let (Ok(x), Ok(y)) = (coords[0].trim().parse::<f64>(), coords[1].trim().parse::<f64>()) {
                return Some(((x.to_bits(), 0i16, 0i8), (y.to_bits(), 0i16, 0i8)));
            }
        }
    }
    None
}

fn parse_box_string(s: &str) -> Option<(((u64, i16, i8), (u64, i16, i8)), ((u64, i16, i8), (u64, i16, i8)))> {
    // Parse format: "((x1,y1),(x2,y2))"
    let s = s.trim();
    if s.starts_with("((") && s.ends_with("))") {
        let inner = &s[2..s.len() - 2];
        let points: Vec<&str> = inner.split("),(").collect();
        if points.len() == 2 {
            let p1_str = points[0].trim_start_matches('(');
            let p2_str = points[1].trim_end_matches(')');
            if let (Some(p1), Some(p2)) = (parse_point_string(&format!("({})", p1_str)), parse_point_string(&format!("({})", p2_str))) {
                return Some((p1, p2));
            }
        }
    }
    None
}

fn parse_circle_string(s: &str) -> Option<(((u64, i16, i8), (u64, i16, i8)), (u64, i16, i8))> {
    // Parse format: "((x,y),r)"
    let s = s.trim();
    if s.starts_with("((") && s.ends_with(')') {
        if let Some(comma_idx) = s.rfind(',') {
            let center_str = &s[1..comma_idx];
            let radius_str = &s[comma_idx + 1..s.len() - 1];
            if let (Some(center), Ok(radius)) = (parse_point_string(center_str), radius_str.trim().parse::<f64>()) {
                return Some((center, (radius.to_bits(), 0i16, 0i8)));
            }
        }
    }
    None
}

fn parse_path_string(s: &str) -> Option<Vec<((u64, i16, i8), (u64, i16, i8))>> {
    // Parse format: "[(x1,y1),(x2,y2),...]" or "((x1,y1),(x2,y2),...)" for closed path
    let s = s.trim();
    let is_closed = s.starts_with("((");
    let inner = if s.starts_with('[') && s.ends_with(']') {
        &s[1..s.len() - 1]
    } else if is_closed {
        &s[1..s.len() - 1]
    } else {
        return None;
    };
    
    let mut points = Vec::new();
    let mut current = String::new();
    let mut depth = 0;
    
    for ch in inner.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth -= 1;
                current.push(ch);
                if depth == 0 {
                    if let Some(point) = parse_point_string(&current) {
                        points.push(point);
                    }
                    current.clear();
                }
            }
            ',' if depth == 0 => {
                // Skip comma between points
            }
            _ => {
                current.push(ch);
            }
        }
    }
    
    if !points.is_empty() {
        Some(points)
    } else {
        None
    }
}

fn parse_line_string(s: &str) -> Option<(((u64, i16, i8), (u64, i16, i8)), ((u64, i16, i8), (u64, i16, i8)))> {
    // Parse format: "{A,B,C}" representing Ax + By + C = 0
    // Convert to two points on the line
    let s = s.trim();
    if s.starts_with('{') && s.ends_with('}') {
        let coords: Vec<&str> = s[1..s.len() - 1].split(',').collect();
        if coords.len() == 3 {
            if let (Ok(a), Ok(b), Ok(c)) = (
                coords[0].trim().parse::<f64>(),
                coords[1].trim().parse::<f64>(),
                coords[2].trim().parse::<f64>(),
            ) {
                // Find two points on the line
                let x1: f64 = 0.0;
                let y1: f64 = if b != 0.0 { -c / b } else { 0.0 };
                let x2: f64 = if a != 0.0 { -c / a } else { 1.0 };
                let y2: f64 = if b != 0.0 { -(a * x2 + c) / b } else { 0.0 };
                return Some((
                    ((x1.to_bits(), 0i16, 0i8), (y1.to_bits(), 0i16, 0i8)),
                    ((x2.to_bits(), 0i16, 0i8), (y2.to_bits(), 0i16, 0i8)),
                ));
            }
        }
    }
    None
}

fn parse_lseg_string(s: &str) -> Option<(((u64, i16, i8), (u64, i16, i8)), ((u64, i16, i8), (u64, i16, i8)))> {
    // Parse format: "[(x1,y1),(x2,y2)]"
    parse_box_string(s)
}

fn parse_polygon_string(s: &str) -> Option<Vec<((u64, i16, i8), (u64, i16, i8))>> {
    // Parse format: "((x1,y1),(x2,y2),...)"
    parse_path_string(s)
}

fn parse_bit_string(s: &str, _is_fixed: bool) -> Option<(u32, Vec<u8>)> {
    // Parse format: "B'1010'" or "'1010'::bit(4)"
    let s = s.trim();
    if s.starts_with("B'") && s.ends_with('\'') {
        let bits_str = &s[2..s.len() - 1];
        let len = bits_str.len();
        let mut bytes = Vec::new();
        let mut current_byte = 0u8;
        let mut bit_pos = 0;
        
        for (i, ch) in bits_str.chars().enumerate() {
            match ch {
                '0' => {
                    // bit is 0, don't set
                }
                '1' => {
                    current_byte |= 1 << (7 - bit_pos);
                }
                _ => return None,
            }
            bit_pos += 1;
            if bit_pos == 8 || i == len - 1 {
                bytes.push(current_byte);
                current_byte = 0;
                bit_pos = 0;
            }
        }
        
        Some((len as u32, bytes))
    } else {
        None
    }
}

fn parse_varbit_string(s: &str) -> Option<(Option<u32>, Vec<u8>)> {
    // Parse format: "B'1010'" for varbit
    if let Some((len, bytes)) = parse_bit_string(s, false) {
        Some((Some(len), bytes))
    } else {
        None
    }
}

fn parse_pg_lsn_string(s: &str) -> Option<u64> {
    // Parse format: "0/12345678" -> 0x12345678
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() == 2 {
        if let (Ok(_high), Ok(low)) = (u32::from_str_radix(parts[0], 16), u32::from_str_radix(parts[1], 16)) {
            return Some(low as u64);
        }
    }
    None
}

fn parse_hstore_string(s: &str) -> Option<Vec<(String, Option<String>)>> {
    // Parse format: "a=>1, b=>2" or "a=>NULL"
    let mut entries = Vec::new();
    let parts: Vec<&str> = s.split(',').collect();
    
    for part in parts {
        let part = part.trim();
        if let Some(arrow_idx) = part.find("=>") {
            let key = part[..arrow_idx].trim().trim_matches('"').to_string();
            let value_str = part[arrow_idx + 2..].trim();
            let value = if value_str.eq_ignore_ascii_case("NULL") {
                None
            } else {
                Some(value_str.trim_matches('"').to_string())
            };
            entries.push((key, value));
        }
    }
    
    if !entries.is_empty() {
        Some(entries)
    } else {
        None
    }
}

fn parse_array_value(array: &pg_types::Array) -> Option<types::PgValue> {
    use fallible_iterator::FallibleIterator;
    
    let element_oid = array.element_type();
    let mut value_iter = array.values();
    
    // Collect elements based on type
    match element_oid {
        16 => {
            // Bool array
            let mut bools = Vec::new();
            loop {
                match value_iter.next() {
                    Ok(Some(Some(value_bytes))) => {
                        if let Ok(b) = pg_types::bool_from_sql(value_bytes) {
                            bools.push(b);
                        } else if let Ok(s) = std::str::from_utf8(value_bytes) {
                            if s == "t" || s == "true" || s == "TRUE" || s == "1" {
                                bools.push(true);
                            } else if s == "f" || s == "false" || s == "FALSE" || s == "0" {
                                bools.push(false);
                            }
                        }
                    }
                    Ok(Some(None)) => {
                        // NULL element - skip since WIT arrays don't support NULLs
                        continue;
                    }
                    Ok(None) | Err(_) => break,
                }
            }
            // Return array even if empty (empty arrays are valid)
            return Some(types::PgValue::BoolArray(bools));
        }
        21 => {
            // Int2 array
            let mut ints = Vec::new();
            loop {
                match value_iter.next() {
                    Ok(Some(Some(value_bytes))) => {
                        if let Ok(i) = pg_types::int2_from_sql(value_bytes) {
                            ints.push(i);
                        } else if let Ok(s) = std::str::from_utf8(value_bytes) {
                            if let Ok(i) = s.parse::<i16>() {
                                ints.push(i);
                            }
                        }
                    }
                    Ok(Some(None)) => continue, // Skip NULL elements
                    Ok(None) | Err(_) => break,
                }
            }
            return Some(types::PgValue::Int2Array(ints));
        }
        23 => {
            // Int4 array
            let mut ints = Vec::new();
            loop {
                match value_iter.next() {
                    Ok(Some(Some(value_bytes))) => {
                        if let Ok(i) = pg_types::int4_from_sql(value_bytes) {
                            ints.push(i);
                        } else if let Ok(s) = std::str::from_utf8(value_bytes) {
                            if let Ok(i) = s.parse::<i32>() {
                                ints.push(i);
                            }
                        }
                    }
                    Ok(Some(None)) => continue, // Skip NULL elements
                    Ok(None) | Err(_) => break,
                }
            }
            return Some(types::PgValue::Int4Array(ints));
        }
        20 => {
            // Int8 array
            let mut ints = Vec::new();
            loop {
                match value_iter.next() {
                    Ok(Some(Some(value_bytes))) => {
                        if let Ok(i) = pg_types::int8_from_sql(value_bytes) {
                            ints.push(i);
                        } else if let Ok(s) = std::str::from_utf8(value_bytes) {
                            if let Ok(i) = s.parse::<i64>() {
                                ints.push(i);
                            }
                        }
                    }
                    Ok(Some(None)) => continue, // Skip NULL elements
                    Ok(None) | Err(_) => break,
                }
            }
            return Some(types::PgValue::Int8Array(ints));
        }
        700 => {
            // Float4 array
            let mut floats = Vec::new();
            loop {
                match value_iter.next() {
                    Ok(Some(Some(value_bytes))) => {
                        if let Ok(f) = pg_types::float4_from_sql(value_bytes) {
                            floats.push((f.to_bits() as u64, 0i16, 0i8));
                        } else if let Ok(s) = std::str::from_utf8(value_bytes) {
                            if let Ok(f) = s.parse::<f32>() {
                                floats.push((f.to_bits() as u64, 0i16, 0i8));
                            }
                        }
                    }
                    Ok(Some(None)) => continue, // Skip NULL elements
                    Ok(None) | Err(_) => break,
                }
            }
            return Some(types::PgValue::Float4Array(floats));
        }
        701 => {
            // Float8 array
            let mut floats = Vec::new();
            loop {
                match value_iter.next() {
                    Ok(Some(Some(value_bytes))) => {
                        if let Ok(f) = pg_types::float8_from_sql(value_bytes) {
                            floats.push((f.to_bits(), 0i16, 0i8));
                        } else if let Ok(s) = std::str::from_utf8(value_bytes) {
                            if let Ok(f) = s.parse::<f64>() {
                                floats.push((f.to_bits(), 0i16, 0i8));
                            }
                        }
                    }
                    Ok(Some(None)) => continue, // Skip NULL elements
                    Ok(None) | Err(_) => break,
                }
            }
            return Some(types::PgValue::Float8Array(floats));
        }
        25 | 1043 | 1042 | 19 => {
            // Text array
            let mut texts = Vec::new();
            loop {
                match value_iter.next() {
                    Ok(Some(Some(value_bytes))) => {
                        if let Ok(s) = pg_types::text_from_sql(value_bytes) {
                            texts.push(s.to_string());
                        } else if let Ok(s) = std::str::from_utf8(value_bytes) {
                            texts.push(s.to_string());
                        }
                    }
                    Ok(Some(None)) => continue, // Skip NULL elements
                    Ok(None) | Err(_) => break,
                }
            }
            return Some(types::PgValue::TextArray(texts));
        }
        17 => {
            // Bytea array
            let mut byteas = Vec::new();
            loop {
                match value_iter.next() {
                    Ok(Some(Some(value_bytes))) => {
                        let decoded = pg_types::bytea_from_sql(value_bytes);
                        byteas.push(decoded.to_vec());
                    }
                    Ok(Some(None)) => continue, // Skip NULL elements
                    Ok(None) | Err(_) => break,
                }
            }
            return Some(types::PgValue::ByteaArray(byteas));
        }
        2950 => {
            // Uuid array
            let mut uuids = Vec::new();
            loop {
                match value_iter.next() {
                    Ok(Some(Some(value_bytes))) => {
                        if let Ok(uuid_bytes) = pg_types::uuid_from_sql(value_bytes) {
                            let uuid = Uuid::from_bytes(uuid_bytes);
                            uuids.push(uuid.to_string());
                        } else if let Ok(s) = std::str::from_utf8(value_bytes) {
                            if Uuid::parse_str(s).is_ok() {
                                uuids.push(s.to_string());
                            }
                        }
                    }
                    Ok(Some(None)) => continue, // Skip NULL elements
                    Ok(None) | Err(_) => break,
                }
            }
            return Some(types::PgValue::UuidArray(uuids));
        }
        114 => {
            // Json array
            let mut jsons = Vec::new();
            loop {
                match value_iter.next() {
                    Ok(Some(Some(value_bytes))) => {
                        if let Ok(s) = std::str::from_utf8(value_bytes) {
                            if serde_json::from_str::<serde_json::Value>(s).is_ok() {
                                jsons.push(s.to_string());
                            }
                        }
                    }
                    Ok(Some(None)) => continue, // Skip NULL elements
                    Ok(None) | Err(_) => break,
                }
            }
            return Some(types::PgValue::JsonArray(jsons));
        }
        3802 => {
            // Jsonb array
            let mut jsonbs = Vec::new();
            loop {
                match value_iter.next() {
                    Ok(Some(Some(value_bytes))) => {
                        if let Ok(s) = std::str::from_utf8(value_bytes) {
                            if serde_json::from_str::<serde_json::Value>(s).is_ok() {
                                jsonbs.push(s.to_string());
                            }
                        }
                    }
                    Ok(Some(None)) => continue, // Skip NULL elements
                    Ok(None) | Err(_) => break,
                }
            }
            return Some(types::PgValue::JsonbArray(jsonbs));
        }
        _ => {
            // For other types, try to parse as text array
            let mut texts = Vec::new();
            loop {
                match value_iter.next() {
                    Ok(Some(Some(value_bytes))) => {
                        if let Ok(s) = std::str::from_utf8(value_bytes) {
                            texts.push(s.to_string());
                        }
                    }
                    Ok(Some(None)) => continue, // Skip NULL elements
                    Ok(None) | Err(_) => break,
                }
            }
            return Some(types::PgValue::TextArray(texts));
        }
    }
    
    None
}

fn parse_tsvector_string(s: &str) -> Option<Vec<types::Lexeme>> {
    // Parse format: "'word':1A 'another':2B"
    let mut lexemes = Vec::new();
    let mut current_word = String::new();
    let mut in_quotes = false;
    let mut current_pos: Option<u16> = None;
    let mut current_weight: Option<types::LexemeWeight> = None;
    
    for ch in s.chars() {
        match ch {
            '\'' if !in_quotes => {
                in_quotes = true;
                current_word.clear();
            }
            '\'' if in_quotes => {
                in_quotes = false;
            }
            ':' if !in_quotes && !current_word.is_empty() => {
                // Position follows
            }
            'A' | 'B' | 'C' | 'D' if !in_quotes => {
                current_weight = match ch {
                    'A' => Some(types::LexemeWeight::A),
                    'B' => Some(types::LexemeWeight::B),
                    'C' => Some(types::LexemeWeight::C),
                    'D' => Some(types::LexemeWeight::D),
                    _ => None,
                };
            }
            c if in_quotes => {
                current_word.push(c);
            }
            c if c.is_ascii_digit() && !in_quotes => {
                let digit_str = c.to_string();
                if let Ok(pos) = digit_str.parse::<u16>() {
                    current_pos = Some(pos);
                }
            }
            ' ' if !in_quotes && !current_word.is_empty() => {
                // End of lexeme
                lexemes.push(types::Lexeme {
                    position: current_pos,
                    weight: current_weight,
                    data: current_word.clone(),
                });
                current_word.clear();
                current_pos = None;
                current_weight = None;
            }
            _ => {}
        }
    }
    
    // Add last lexeme
    if !current_word.is_empty() {
        lexemes.push(types::Lexeme {
            position: current_pos,
            weight: current_weight,
            data: current_word,
        });
    }
    
    if !lexemes.is_empty() {
        Some(lexemes)
    } else {
        None
    }
}

fn pg_date_to_wit_date(days: i32) -> types::Date {
    if days == i32::MAX {
        return types::Date::PositiveInfinity;
    }
    if days == i32::MIN {
        return types::Date::NegativeInfinity;
    }

    let base_date =
        Date::from_calendar_date(2000, time::Month::January, 1).expect("invalid base date");
    let date = base_date + time::Duration::days(days as i64);

    types::Date::Ymd((date.year(), date.month() as u32, date.day() as u32))
}

fn pg_time_to_wit_time(microseconds: i64) -> types::Time {
    let seconds = microseconds / 1_000_000;
    let micros = (microseconds % 1_000_000) as u32;

    let hour = (seconds / 3600) as u32;
    let min = ((seconds % 3600) / 60) as u32;
    let sec = (seconds % 60) as u32;

    types::Time {
        hour,
        min,
        sec,
        micro: micros,
    }
}
