class CreateCurrentDirectoryItems < ActiveRecord::Migration
  def change
    create_table :current_directory_items, force: true do |t|
      t.integer :current_period_type, null: false
      t.references :user, null: false 
      t.integer :topic_count, null: false, default: 0
      t.integer :post_count, null: false, default: 0
      t.integer :total_participation, null: false, default: 0
      t.timestamps
    end
    
    add_index :current_directory_items, [:current_period_type, :user_id], unique: true, name: 'index_current_directory_items_on_period_type_and_user'
  end
end