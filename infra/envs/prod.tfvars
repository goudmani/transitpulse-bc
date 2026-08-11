env                      = "prod"
region                   = "ca-central-1"
owner                    = "manikanth"
alert_email              = "CHANGE_ME@example.com"
poller_package_type      = "Zip" # "Image" if you prefer the container build
poller_image_tag         = "v1"
daily_cost_threshold_usd = 5
force_destroy_buckets    = false

github_repo = "" # set to "yourusername/transitpulse-bc" once the repo exists
