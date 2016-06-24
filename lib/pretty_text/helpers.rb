module PrettyText
  module Helpers
    extend self

    # functions here are available to v8

    def avatar_template(username)
      return "" unless username
      user = User.find_by(username_lower: username.downcase)
      return "" unless user.present?

      # TODO: Add support for ES6 and call `avatar-template` directly
      if !user.uploaded_avatar_id
        avatar_template = User.default_template(username)
      else
        avatar_template = user.avatar_template
      end

      UrlHelper.schemaless UrlHelper.absolute avatar_template
    end

    def mention_lookup(username)
      return false unless username
      if Group.exec_sql('SELECT 1 FROM groups WHERE name = ?', username).values.length == 1
        "group"
      else
        username = username.downcase
        if User.exec_sql('SELECT 1 FROM users WHERE username_lower = ?', username).values.length == 1
          "user"
        else
          nil
        end
      end
    end

    def category_hashtag_lookup(category_slug)
      if category = Category.query_from_hashtag_slug(category_slug)
        [category.url_with_id, category_slug]
      else
        nil
      end
    end

    def get_topic_info(topic_id)
      return unless Fixnum === topic_id
      # TODO this only handles public topics, secured one do not get this
      topic = Topic.find_by(id: topic_id)
      if topic && Guardian.new.can_see?(topic)
        {
          title: topic.title,
          href: topic.url
        }
      end
    end

    def category_tag_hashtag_lookup(text)
      tag_postfix = '::tag'
      is_tag = text =~ /#{tag_postfix}$/

      if !is_tag && category = Category.query_from_hashtag_slug(text)
        [category.url_with_id, text]
      elsif is_tag && tag = Tag.find_by_name(text.gsub!("#{tag_postfix}", ''))
        ["#{Discourse.base_url}/tags/#{tag.name}", text]
      else
        nil
      end
    end
  end
end

