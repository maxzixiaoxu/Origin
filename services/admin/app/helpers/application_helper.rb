module ApplicationHelper
  def nav_class(controller_name)
    params[:controller].to_s.start_with?(controller_name) ? "on" : nil
  end

  # Status pill. The colour mapping matters more than it looks: 'failed' is
  # amber rather than red because it is transient -- the job is between a failed
  # attempt and its scheduled retry. Showing it as red would make a healthy,
  # self-recovering queue look broken.
  def status_badge(status)
    css = case status
          when "succeeded"           then "ok"
          when "dead", "cancelled"   then "bad"
          when "failed"              then "warn"
          when "running"             then "live"
          else                            "idle"
          end
    tag.span(status, class: "badge #{css}")
  end

  def outcome_badge(outcome)
    return tag.span("—", class: "badge idle") if outcome.blank?

    css = case outcome
          when "succeeded"                then "ok"
          when "lease_expired", "timeout" then "warn"
          else                                 "bad"
          end
    tag.span(outcome.tr("_", " "), class: "badge #{css}")
  end

  def ms(value)
    return "—" if value.blank?

    value < 1000 ? "#{value} ms" : "#{(value / 1000.0).round(2)} s"
  end

  def bytes(value)
    return "—" if value.blank?

    number_to_human_size(value)
  end

  def ago(time)
    return "—" if time.blank?

    tag.span("#{time_ago_in_words(time)} ago", title: time.utc.iso8601)
  end

  def short_id(id)
    return "—" if id.blank?

    tag.code(id.to_s.first(8), title: id)
  end
end
