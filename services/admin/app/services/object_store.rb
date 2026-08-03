# S3-compatible object storage for image originals and their derivatives.
#
# Uses the AWS SDK against MinIO, matching what the Go workers do, so the two
# sides agree on bucket, key layout, and path-style addressing.
class ObjectStore
  class Error < StandardError; end

  # Uploads are capped well below the worker's own 32 MiB limit. Rejecting an
  # oversized file at the door beats accepting it, queueing three jobs, and
  # having all three dead-letter a second later.
  MAX_UPLOAD_BYTES = 25 * 1024 * 1024

  ALLOWED_CONTENT_TYPES = %w[image/jpeg image/png image/webp].freeze

  def initialize
    @bucket = ENV.fetch("S3_BUCKET", "jobq")
  end

  attr_reader :bucket

  def client
    @client ||= Aws::S3::Client.new(
      endpoint: ENV.fetch("S3_ENDPOINT", "http://localhost:59000"),
      region: ENV.fetch("S3_REGION", "us-east-1"),
      access_key_id: ENV.fetch("S3_ACCESS_KEY", "jobq"),
      secret_access_key: ENV.fetch("S3_SECRET_KEY", "jobq-secret"),
      # MinIO has no wildcard DNS, so bucket-in-hostname addressing cannot
      # resolve. Harmless against real S3.
      force_path_style: true
    )
  end

  # Presigning uses a SEPARATE client bound to the browser-facing endpoint.
  #
  # Inside Docker, Rails reaches MinIO at http://minio:9000, but a URL signed
  # for that host is useless to a browser on the host machine -- it cannot
  # resolve "minio". The signature covers the host header, so the URL cannot be
  # string-rewritten after the fact; it has to be signed against the public
  # address in the first place.
  def presign_client
    @presign_client ||= begin
      public_endpoint = ENV["S3_PUBLIC_ENDPOINT"].presence
      public_endpoint ? build_client(public_endpoint) : client
    end
  end

  def upload(key:, io:, content_type:)
    client.put_object(bucket:, key:, body: io, content_type:)
    key
  rescue Aws::Errors::ServiceError => e
    raise Error, "could not upload #{key}: #{e.message}"
  end

  # A time-limited URL the browser fetches directly.
  #
  # Serving images this way keeps Rails out of the bytes path entirely -- it
  # hands out a URL and the browser talks to object storage. Proxying would tie
  # up a Puma thread for the duration of every image download, and a grid of
  # thumbnails would exhaust the pool.
  def presigned_url(key, expires_in: 3600)
    return nil if key.blank?

    Aws::S3::Presigner
      .new(client: presign_client)
      .presigned_url(:get_object, bucket:, key:, expires_in:)
  rescue Aws::Errors::ServiceError => e
    Rails.logger.warn("could not presign #{key}: #{e.message}")
    nil
  end

  def exists?(key)
    client.head_object(bucket:, key:)
    true
  rescue Aws::S3::Errors::NotFound, Aws::S3::Errors::NoSuchKey
    false
  end

  def healthy?
    client.head_bucket(bucket:)
    true
  rescue Aws::Errors::ServiceError, Seahorse::Client::NetworkingError
    false
  end

  private

  def build_client(endpoint)
    Aws::S3::Client.new(
      endpoint:,
      region: ENV.fetch("S3_REGION", "us-east-1"),
      access_key_id: ENV.fetch("S3_ACCESS_KEY", "jobq"),
      secret_access_key: ENV.fetch("S3_SECRET_KEY", "jobq-secret"),
      force_path_style: true
    )
  end
end
