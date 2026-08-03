# Turns one uploaded image into three queued derivative jobs.
#
# This is the fan-out that makes priority scheduling visible: all three jobs are
# submitted in the same instant, but the thumbnail is priority 0 because a user
# is waiting on it, and the archive size is priority 9 because nobody is. The
# thumb reliably lands first, which is something you can watch rather than
# something a README asserts.
class ImageSubmission
  # Mirrors jobspec.StandardSizes in Go. The Go constant is authoritative for
  # the workers; this list is what the submitter asks for. They must agree.
  SIZES = [
    { label: "thumb",  width: 150,  priority: 0 },
    { label: "medium", width: 640,  priority: 5 },
    { label: "large",  width: 1600, priority: 9 }
  ].freeze

  QUEUE = "images".freeze
  JOB_TYPE = "image.derive".freeze

  Result = Struct.new(:source_key, :jobs, :errors, keyword_init: true) do
    def success? = errors.empty?
  end

  def initialize(store: ObjectStore.new, client: QueueClient.new)
    @store = store
    @client = client
  end

  def call(uploaded_file)
    validate!(uploaded_file)

    source_key = build_source_key(uploaded_file.original_filename)
    @store.upload(key: source_key, io: uploaded_file.tempfile,
                  content_type: uploaded_file.content_type)

    response = @client.enqueue_batch(build_jobs(source_key))
    parse(response, source_key)
  end

  private

  def validate!(file)
    raise ArgumentError, "no file was selected" if file.blank?

    unless ALLOWED_TYPES.include?(file.content_type)
      raise ArgumentError,
            "#{file.content_type} is not a supported image type " \
            "(#{ALLOWED_TYPES.join(', ')})"
    end

    if file.size > ObjectStore::MAX_UPLOAD_BYTES
      raise ArgumentError,
            "file is #{ActiveSupport::NumberHelper.number_to_human_size(file.size)}, " \
            "above the #{ActiveSupport::NumberHelper.number_to_human_size(ObjectStore::MAX_UPLOAD_BYTES)} limit"
    end
  end

  ALLOWED_TYPES = ObjectStore::ALLOWED_CONTENT_TYPES

  # Keys carry a timestamp and a random suffix.
  #
  # The random suffix matters: two people uploading "photo.jpg" in the same
  # millisecond would otherwise collide, and the second upload would silently
  # overwrite the first while both sets of jobs pointed at the same object.
  def build_source_key(filename)
    stamp = Time.current.utc.strftime("%Y%m%dT%H%M%S%L")
    safe = File.basename(filename.to_s).gsub(/[^a-zA-Z0-9._-]/, "_").presence || "upload"
    "originals/#{stamp}-#{SecureRandom.hex(4)}-#{safe}"
  end

  def build_jobs(source_key)
    base = File.basename(source_key, File.extname(source_key))

    SIZES.map do |size|
      {
        queue: QUEUE,
        type: JOB_TYPE,
        priority: size[:priority],
        payload: {
          source_key:,
          target_key: "derivatives/#{base}-#{size[:label]}.jpg",
          width: size[:width],
          label: size[:label],
          format: "jpeg"
        }
      }
    end
  end

  def parse(response, source_key)
    jobs = []
    errors = []

    Array(response["results"]).each_with_index do |item, index|
      label = SIZES.dig(index, :label)

      if item["error"].present?
        errors << "#{label}: #{item['error']}"
        next
      end

      jobs << {
        id: item.dig("response", "id"),
        label:,
        width: SIZES.dig(index, :width),
        priority: SIZES.dig(index, :priority)
      }
    end

    Result.new(source_key:, jobs:, errors:)
  end
end
