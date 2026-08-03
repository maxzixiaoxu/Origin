Rails.application.routes.draw do
  root "dashboard#show"

  get "dashboard/stats", to: "dashboard#stats", as: :dashboard_stats

  # The demo entry point. Keyed by a job UUID, not the S3 key: the key contains
  # slashes, and escaping it produced double-escaped URLs like
  # /uploads/originals%252F... because Rails re-escapes an escaped value.
  resources :uploads, only: %i[new create show] do
    member { get :status }
  end

  resources :jobs, only: %i[index show] do
    member do
      post :cancel
      post :retry
    end
  end

  # Queues are keyed by name, not id.
  resources :queues, only: %i[index update], param: :name, constraints: { name: /[a-z0-9][a-z0-9_.\-]*/ } do
    member do
      post :pause
      post :resume
    end
  end

  resources :dead_letter, only: :index do
    collection { post :bulk_retry }
  end

  # Liveness for the container healthcheck. Deliberately does not touch
  # Postgres, Redis, or the queue service: an orchestrator killing this
  # container because a dependency blipped helps nobody.
  get "up", to: proc { [200, {}, ["ok\n"]] }
end
