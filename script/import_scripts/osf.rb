if ARGV.length != 1 || !File.exists?(ARGV[0])
    STDERR.puts 'Usage of OSF importer:', 'bundle exec ruby osf.rb <path to osf export file>'
    STDERR.puts 'Make sure the export file exists' if ARGV.length == 1 && !File.exists?(ARGV[0])
    exit 1
end

require File.expand_path(File.dirname(__FILE__) + "/base.rb")

require 'yajl' # streaming JSON input
require 'pry'

class ImportScripts::Osf < ImportScripts::Base
    BATCH_SIZE = 1000
    CATEGORY_COLORS = ['BF1E2E', '3AB54A', '652D90']

    def initialize
        super
    end

    def import_objects(objects, object_type)
        if object_type == 'user'
            import_users(objects)
        elsif object_type == 'project'
            import_groups(objects)
        elsif object_type == 'post'
            import_posts(objects)
        end
    end

    def execute
        import_categories

        objects = []
        object_type = nil

        pipe_file = File.new(ARGV[0], 'r')
        Yajl::Parser.parse(pipe_file) do |obj|
            if object_type && (object_type != obj['object_type'] || objects.length > BATCH_SIZE)
                import_objects(objects, object_type)
                objects = []
            end
            object_type = obj['object_type']
            objects << obj
        end

        import_objects(objects, object_type) if objects.length > 0
    end

    def import_categories
        puts "", "importing categories..."
        create_categories([0, 1, 2]) do |i|
            {
                id: ["files", "wiki", "node"][i],
                name: ["File", "Wiki", "Project"][i],
                color: CATEGORY_COLORS[i]
            }
        end
    end

    def import_users(users)
        puts '', "creating users"
        create_users(users) do |user|
            {
                id: user['id'],
                email: user['email'],
                username: user['username'],
                name: user['name'],
                avatar_url: user['avatar_url'],
            }
        end
    end

    def import_groups(projects)
        puts '', "creating groups"
        create_groups(projects) do |project|
            {
                id: project['id'],
                name: project['guid'],
                visible: project['is_public'],
            }
        end
        projects.each do |project|
            group = find_group_by_import_id(project['id'])
            group.bulk_add(project['contributors'].map { |u| user_id_from_imported_user_id(u) } )
            raise "Visibility failed to import to group: " unless group.visible == project['is_public']
        end
    end

    def import_posts(posts)
        puts "", "creating topics and posts"

        create_posts(posts) do |post|
            if post['post_type'] == 'topic'
                {
                    id: post['id'],
                    title: post['title'],
                    raw: post['content'],
                    user_id: -1, #system
                    created_at: Time.parse(post['date_created']),
                    category: category_id_from_imported_category_id(post['type']),
                    #custom_fields: {
                    #    topic_guid: post['topic_guid'],
                    #    parent_guids: post['parent_guids'],
                    #}
                }
            else
                parent = topic_lookup_from_imported_post_id(post['reply_to'])
                {
                    id: post['id'],
                    raw: post['content'],
                    user_id: user_id_from_imported_user_id(post['user']),
                    topic_id: parent[:topic_id],
                    reply_to_post_number: parent[:post_number],
                    created_at: Time.parse(post['date_created']),
                }
            end
        end

        posts.each do |post_data|
            next unless post_data['post_type'] == 'topic'
            topic_data = topic_lookup_from_imported_post_id(post_data['id'])
            #post_id = post_id_from_imported_post_id(post_data['id'])

            #topic = Topic.where()
            #post = Post.find(post_id)
            #post.custom_fields['parent_guids'] = post_data['parent_guids']
            #post.custom_fields['topic_guid'] = post_data['topic_guid']
            #post.save

            topic = Topic.find(topic_data[:topic_id])
            # parent_guids array gets reversed on insertion to DB during save.
            topic.custom_fields['parent_guids'] = post_data['parent_guids'].reverse
            topic.custom_fields['project_guid'] = post_data['parent_guids'][0]
            topic.custom_fields['topic_guid'] = post_data['topic_guid']
            topic.save

            parent_guids = topic.custom_fields['parent_guids']#PostCustomField.where(post_id: post_id, name: 'parent_guids').pluck(:value).first
            project_guid = topic.custom_fields['project_guid']
            topic_guid = topic.custom_fields['topic_guid']#PostCustomField.where(post_id: post_id, name: 'topic_guid').pluck(:value).first
            raise "Parent guids did not persist, #{parent_guids} != #{post_data['parent_guids']}" unless [parent_guids].flatten == post_data['parent_guids']
            raise "Project guid did not persist" unless project_guid == post_data['parent_guids'][0]
            raise "Topic guid did not persist" unless topic_guid == post_data['topic_guid']
        end

    end

end

ImportScripts::Osf.new.perform
