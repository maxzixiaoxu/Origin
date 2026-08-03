# HTTP client for the Go queue service.
#
# This is the ONLY way Rails writes anything. Models are read-only; every state
# change -- enqueue, cancel, retry, pause -- goes through here, because the Go
# service owns the Redis lease set and is the only component that can move a job
# between states safely.
class QueueClient
  class Error < StandardError
    attr_reader :status, :code, :body

    def initialize(message, status: nil, code: nil, body: nil)
      super(message)
      @status = status
      @code = code
      @body = body
    end
  end

  # Raised when the job exists but is not in a state the operation allows --
  # retrying a running job, for instance. Distinct from Error so controllers can
  # show "that is not possible right now" rather than "something broke".
  class ConflictError < Error; end
  class NotFoundError < Error; end

  def initialize(base_url: nil, logger: Rails.logger)
    @base_url = base_url || ENV.fetch("QUEUE_API_URL", "http://localhost:58080")
    @logger = logger
  end

  # --- jobs ---------------------------------------------------------------

  def enqueue(queue:, type:, payload: {}, priority: nil, max_attempts: nil,
              idempotency_key: nil, run_at: nil)
    body = { queue:, type:, payload: }
    body[:priority] = priority unless priority.nil?
    body[:max_attempts] = max_attempts unless max_attempts.nil?
    body[:idempotency_key] = idempotency_key if idempotency_key.present?
    body[:run_at] = run_at.utc.iso8601 if run_at.present?

    post("/v1/jobs", body)
  end

  # Submits several jobs in one call, returning per-item results.
  #
  # The image upload uses this: one request creates all three derivative jobs.
  # Three sequential requests would let a Rails crash mid-loop leave an image
  # with only some of its sizes queued, and nothing would ever notice.
  def enqueue_batch(jobs)
    post("/v1/jobs/batch", { jobs: })
  end

  def job(id, attempts: false)
    get("/v1/jobs/#{id}", attempts ? { attempts: true } : {})
  end

  def cancel(id)
    post("/v1/jobs/#{id}/cancel")
  end

  # Returns a dead or failed job to the ready set.
  #
  # reset defaults to true: a human clicking "retry" wants the job to actually
  # run, not to fail once more against an attempt budget that is already spent.
  def retry_job(id, reset: true)
    post("/v1/jobs/#{id}/retry", nil, query: { reset: reset.to_s })
  end

  # --- queues -------------------------------------------------------------

  def queues
    get("/v1/queues").fetch("queues", [])
  end

  # Partial update. Unspecified settings are left alone by the server, so the UI
  # can send only what changed -- which avoids a lost-update race between two
  # operators editing the same queue, precisely when two people are most likely
  # to be doing so.
  def update_queue(name, attrs)
    put("/v1/queues/#{name}", attrs.compact)
  end

  def pause(name)  = post("/v1/queues/#{name}/pause")
  def resume(name) = post("/v1/queues/#{name}/resume")

  def depth(queues = [])
    query = queues.map { |q| ["queue", q] }
    get("/v1/depth", query).fetch("depths", [])
  end

  def healthy?
    connection.get("/healthz").success?
  rescue Faraday::Error
    false
  end

  private

  attr_reader :base_url, :logger

  def connection
    @connection ||= Faraday.new(url: base_url) do |f|
      f.request :json
      f.response :json, content_type: /\bjson$/

      # Retries only idempotent methods and only on transport-level failures.
      #
      # POST is excluded deliberately. Retrying an enqueue that actually
      # succeeded but whose response was lost would create a duplicate job --
      # unless the caller set an idempotency key, and the client cannot know
      # that. Better to surface the error and let the caller decide.
      f.request :retry,
                max: 2,
                interval: 0.1,
                backoff_factor: 2,
                methods: %i[get put],
                exceptions: [Faraday::ConnectionFailed, Faraday::TimeoutError]

      f.options.timeout = 10
      f.options.open_timeout = 3
    end
  end

  # Propagates the current request's id so one identifier spans the browser
  # request, the Rails log, the queue service log, the job row, and the worker.
  def trace_headers
    id = Current.request_id
    id.present? ? { "X-Request-Id" => id } : {}
  end

  def get(path, params = {})
    request(:get) { connection.get(path, params, trace_headers) }
  end

  def post(path, body = nil, query: {})
    full = query.present? ? "#{path}?#{query.to_query}" : path
    request(:post) { connection.post(full, body, trace_headers) }
  end

  def put(path, body)
    request(:put) { connection.put(path, body, trace_headers) }
  end

  def request(verb)
    response = yield
    return response.body if response.success?

    raise_for(response)
  rescue Faraday::ConnectionFailed, Faraday::TimeoutError => e
    # The queue service being unreachable is an operational condition the
    # dashboard should render as such, not a 500 with a stack trace.
    logger.error("queue service unreachable (#{verb}): #{e.message}")
    raise Error.new("The queue service is unreachable: #{e.message}")
  end

  def raise_for(response)
    body = response.body.is_a?(Hash) ? response.body : {}
    message = body["error"].presence || "queue service returned #{response.status}"
    code = body["code"]

    case response.status
    when 404 then raise NotFoundError.new(message, status: 404, code:, body:)
    when 409 then raise ConflictError.new(message, status: 409, code:, body:)
    else raise Error.new(message, status: response.status, code:, body:)
    end
  end
end
