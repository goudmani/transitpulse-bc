variable "name" {
  type = string
}

variable "region" {
  type = string
}

variable "alerts_topic_arn" {
  type = string
}

variable "poller_function_name" {
  type = string
}

variable "poller_log_group" {
  type = string
}

variable "poller_dlq_name" {
  type = string
}

variable "kinesis_stream_name" {
  type = string
}

variable "killswitch_function_arn" {
  type = string
}

variable "daily_cost_threshold_usd" {
  type    = number
  default = 3
}
