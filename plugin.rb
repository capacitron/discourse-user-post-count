# name: userpostcount
# about: A simpler user listing on quarterly basis
# version: 0.0.1
# authors: @sushil10018, honestcleaner.com

after_initialize do

  register_asset "javascripts/discourse/userpostcount-route-map.js.es6"

  Discourse::Application.routes.prepend do
    resources :current_directory_items
    get "userpostcount" => 'users#index'
  end

  Site.class_eval do
    def self.json_for(guardian)

      if guardian.anonymous? && SiteSetting.login_required
        return {
          periods: TopTopic.periods.map(&:to_s),
          current_periods: TopTopic.current_periods.map(&:to_s),
          filters: Discourse.filters.map(&:to_s),
          user_fields: UserField.all.map do |userfield|
            UserFieldSerializer.new(userfield, root: false, scope: guardian)
          end
        }.to_json
      end

      seq = nil

      if guardian.anonymous?
        seq = MessageBus.last_id('/site_json')

        cached_json, cached_seq, cached_version = $redis.mget('site_json', 'site_json_seq', 'site_json_version')

        if cached_json && seq == cached_seq.to_i && Discourse.git_version == cached_version
          return cached_json
        end

      end

      site = Site.new(guardian)
      json = MultiJson.dump(SiteSerializer.new(site, root: false, scope: guardian))

      if guardian.anonymous?
        $redis.multi do
          $redis.setex 'site_json', 1800, json
          $redis.set 'site_json_seq', seq
          $redis.set 'site_json_version', Discourse.git_version
        end
      end

      json
    end
  end

  TopTopic.class_eval do
    def self.current_periods
      @@current_periods ||= [:first_quarterly, :second_quarterly, :third_quarterly, :fourth_quarterly].freeze
    end
  end

  SiteSerializer.class_eval do
    attributes :current_periods

    def current_periods
      TopTopic.current_periods.map(&:to_s)
    end
  end

  require_dependency "active_record"
  class ::CurrentDirectoryItem < ::ActiveRecord::Base
    belongs_to :user
    has_one :user_stat, foreign_key: :user_id, primary_key: :user_id

    def self.headings
      @headings ||= [:topic_count,
                     :post_count,
                     :total_participation]
    end

    def self.current_period_types
      @types ||= Enum.new(fourth_quarterly: 1,
                          third_quarterly: 2,
                          second_quarterly: 3,
                          first_quarterly: 4)
    end

    def self.refresh!
      current_period_types.each_key {|p| refresh_period!(p)}
    end

    def self.refresh_period!(current_period_type)

      # Don't calculate it if the user directory is disabled
      return unless SiteSetting.enable_user_directory?

      since = case current_period_type
              when :first_quarterly then Time.zone.now.beginning_of_year
              when :second_quarterly then (Time.zone.now.beginning_of_year + 3.months)
              when :third_quarterly then (Time.zone.now.beginning_of_year + 6.months)
              when :fourth_quarterly then (Time.zone.now.beginning_of_year + 9.months)
              else 1000.years.ago
              end

      ActiveRecord::Base.transaction do
        exec_sql "DELETE FROM current_directory_items
                  USING current_directory_items di
                  LEFT JOIN users u ON u.id = user_id
                  WHERE di.id = current_directory_items.id AND
                        u.id IS NULL AND
                        di.current_period_type = :current_period_type", current_period_type: current_period_types[current_period_type]


        exec_sql "INSERT INTO current_directory_items(current_period_type, user_id, topic_count, post_count, total_participation)
                  SELECT
                      :current_period_type,
                      u.id,
                      0,
                      0,
                      0
                  FROM users u
                  LEFT JOIN current_directory_items di ON di.user_id = u.id AND di.current_period_type = :current_period_type
                  WHERE di.id IS NULL AND u.id > 0
        ", current_period_type: current_period_types[current_period_type]

        exec_sql "WITH x AS (SELECT
                      u.id user_id,
                      SUM(CASE WHEN ua.action_type = :new_topic_type THEN 1 ELSE 0 END) topic_count,
                      SUM(CASE WHEN ua.action_type = :reply_type THEN 1 ELSE 0 END) post_count
                    FROM users AS u
                    LEFT OUTER JOIN user_actions AS ua ON ua.user_id = u.id
                    LEFT OUTER JOIN topics AS t ON ua.target_topic_id = t.id AND t.archetype = 'regular'
                    LEFT OUTER JOIN posts AS p ON ua.target_post_id = p.id
                    LEFT OUTER JOIN categories AS c ON t.category_id = c.id
                    WHERE u.active
                      AND NOT u.blocked
                      AND COALESCE(ua.created_at, :since) >= :since
                      AND t.deleted_at IS NULL
                      AND COALESCE(t.visible, true)
                      AND p.deleted_at IS NULL
                      AND (NOT (COALESCE(p.hidden, false)))
                      AND COALESCE(p.post_type, :regular_post_type) = :regular_post_type
                      AND u.id > 0
                    GROUP BY u.id)
        UPDATE current_directory_items di SET
                 topic_count = x.topic_count,
                 post_count = x.post_count,
                 total_participation = x.topic_count + x.post_count
        FROM x
        WHERE
          x.user_id = di.user_id AND
          di.current_period_type = :current_period_type AND (
          di.topic_count <> x.topic_count OR
          di.post_count <> x.post_count )

        ",
                    current_period_type: current_period_types[current_period_type],
                    since: since,
                    new_topic_type: UserAction::NEW_TOPIC,
                    reply_type: UserAction::REPLY,
                    regular_post_type: Post.types[:regular]
      end
    end
  end

  require_dependency "application_controller"
  class ::CurrentDirectoryItemsController < ::ApplicationController
    PAGE_SIZE = 50

    def index
      raise Discourse::InvalidAccess.new(:enable_user_directory) unless SiteSetting.enable_user_directory?

      current_period = params.require(:current_period)
      current_period_type = CurrentDirectoryItem.current_period_types[current_period.to_sym]
      raise Discourse::InvalidAccess.new(:current_period_type) unless current_period_type

      result = CurrentDirectoryItem.where(current_period_type: current_period_type).includes(:user)

      order = params[:order] || CurrentDirectoryItem.headings.last
      if CurrentDirectoryItem.headings.include?(order.to_sym)
        dir = params[:asc] ? 'ASC' : 'DESC'
        result = result.order("current_directory_items.#{order} #{dir}")
      end

      page = params[:page].to_i

      user_ids = nil
      if params[:name].present?
        user_ids = UserSearch.new(params[:name]).search.pluck(:id)
        if user_ids.present?
          # Add the current user if we have at least one other match
          if current_user && result.dup.where(user_id: user_ids).count > 0
            user_ids << current_user.id
          end
          result = result.where(user_id: user_ids)
        else
          result = result.where('false')
        end
      end

      if params[:username]
        user_id = User.where(username_lower: params[:username].to_s.downcase).pluck(:id).first
        if user_id
          result = result.where(user_id: user_id)
        else
          result = result.where('false')
        end
      end

      result = result.order('users.username')
      result_count = result.dup.count
      result = result.limit(PAGE_SIZE).offset(PAGE_SIZE * page).to_a

      more_params = params.slice(:current_period, :order, :asc)
      more_params[:page] = page + 1

      # Put yourself at the top of the first page
      if result.present? && current_user.present? && page == 0

        position = result.index {|r| r.user_id == current_user.id }

        # Don't show the record unless you're not in the top positions already
        if (position || 10) >= 10
          your_item = CurrentDirectoryItem.where(current_period_type: current_period_type, user_id: current_user.id).first
          result.insert(0, your_item) if your_item
        end

      end

      render_json_dump(current_directory_items: serialize_data(result, CurrentDirectoryItemSerializer),
                       total_rows_directory_items: result_count,
                       load_more_directory_items: current_directory_items_path(more_params))
    end
  end

  require_dependency "application_serializer"
  class ::CurrentDirectoryItemSerializer < ::ApplicationSerializer

    attributes :id,
               :time_read

    has_one :user, embed: :objects, serializer: UserNameSerializer
    attributes *CurrentDirectoryItem.headings

    def id
      object.user_id
    end

    def time_read
      AgeWords.age_words(object.user_stat.time_read)
    end

    def include_time_read?
      object.current_period_type == CurrentDirectoryItem.current_period_types[:first_quarterly]
    end

  end

  class ::Jobs::CurrentDirectoryRefreshDaily < ::Jobs::Scheduled
    every 1.hour

    def execute(args)
      CurrentDirectoryItem.refresh_period!(current_quarter)
    end

    def current_quarter
      quarter = ((Time.zone.now.month - 1) / 3) + 1
      cq = case quarter
        when 1
          :first_quarterly
        when 2
          :second_quarterly
        when 3
          :third_quarterly
        when 4
          :fourth_quarterly
        else
          :first_quarterly
      end
    end
  end

  class ::Jobs::CurrentDirectoryRefreshOlder < ::Jobs::Scheduled
    every 1.day

    def execute(args)
      current_periods = CurrentDirectoryItem.current_period_types.keys - [:first_quarterly]
      current_periods.each {|p| CurrentDirectoryItem.refresh_period!(p)}
    end
  end

end
