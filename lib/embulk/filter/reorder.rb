Embulk::JavaPlugin.register_filter(
  "reorder", "org.embulk.filter.reorder.ReorderFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
