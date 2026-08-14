env                      = "dev"
region                   = "ca-central-1"
owner                    = "manikanth"
alert_email              = "manikanthgoud27@gmail.com"
poller_package_type      = "Zip" # "Image" if you prefer the container build
poller_image_tag         = "v1"
daily_cost_threshold_usd = 3
force_destroy_buckets    = true

# Must match the repo's CURRENT full name exactly -- it becomes the `sub`
# condition on both OIDC trust policies. Do not trust `git remote -v` here: this
# repo was renamed and the remote still shows the old name, which GitHub silently
# redirects. Confirm with:
#   curl -s https://api.github.com/repos/<owner>/<repo> | grep full_name
github_repo = "goudmani/transitpulse-bc"
