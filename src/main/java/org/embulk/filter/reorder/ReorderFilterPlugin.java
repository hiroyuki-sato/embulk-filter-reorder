package org.embulk.filter.reorder;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Types;

import javax.script.ScriptEngineManager;
import java.util.HashMap;
import java.util.List;

public class ReorderFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("columns")
        @ConfigDefault("{}")
        List<String> getColumns();

    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        Schema.Builder builder = Schema.builder();

        for (String columnName : task.getColumns()) {
            Column column = inputSchema.lookupColumn(columnName);
            builder.add(columnName,column.getType());
        }

        control.run(task.dump(), builder.build());
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema inputSchema,
                           final Schema outputSchema, final PageOutput output)
    {
//        PluginTask task = taskSource.loadTask(PluginTask.class);

        return new PageOutput() {
            private final PageReader reader = new PageReader(inputSchema);
            private final PageBuilder builder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);

            @Override
            public void finish()
            {
                builder.finish();
            }

            @Override
            public void close()
            {
                builder.close();
            }

            @Override
            public void add(Page page){
                reader.setPage(page);

                while (reader.nextRecord()) {

                    for (Column column : outputSchema.getColumns()) {
                        Column input = inputSchema.lookupColumn(column.getName());

                        if (reader.isNull(input)) {
                            builder.setNull(column);
                            continue;
                        }
                        if (Types.STRING.equals(column.getType())) {
                            builder.setString(column, reader.getString(input));
                        }
                        else if (Types.BOOLEAN.equals(column.getType())) {
                            builder.setBoolean(column, reader.getBoolean(input));
                        }
                        else if (Types.DOUBLE.equals(column.getType())) {
                            builder.setDouble(column, reader.getDouble(input));
                        }
                        else if (Types.LONG.equals(column.getType())) {
                            builder.setLong(column, reader.getLong(input));
                        }
                        else if (Types.TIMESTAMP.equals(column.getType())) {
                            builder.setTimestamp(column, reader.getTimestamp(input));
                        }
                        else if (Types.JSON.equals(column.getType())) {
                            builder.setJson(column, reader.getJson(input));
                        }

                    }
                    builder.addRecord();
                }
            }

        };
    }
}
