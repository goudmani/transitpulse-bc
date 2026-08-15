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

# This repo was created after 2026-07-15, so GitHub signs the OIDC `sub` claim in
# the immutable form with numeric owner and repo IDs appended. Without this the
# trust policy matches nothing and STS returns "Not authorized to perform
# sts:AssumeRoleWithWebIdentity", which reads exactly like a missing role.
github_repo_immutable = "goudmani@184206526/transitpulse-bc@1326056479"
