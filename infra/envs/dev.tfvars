env                      = "dev"
region                   = "ca-central-1"
owner                    = "manikanth"
alert_email              = "manikanthgoud27@gmail.com"
poller_package_type      = "Zip" # "Image" if you prefer the container build
poller_image_tag         = "v1"
daily_cost_threshold_usd = 3
force_destroy_buckets    = true

github_repo = "goudmani/transitpulse-bc" # set to "yourusername/transitpulse-bc" once the repo exists
