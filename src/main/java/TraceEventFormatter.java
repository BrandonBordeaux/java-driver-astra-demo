import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.TraceEvent;

import java.util.LinkedList;
import java.util.List;

public class TraceEventFormatter {
    private final QueryTrace queryTrace;

    private final String activityLabel = "activity";
    private final String sourceLabel = "source";
    private final String sourceElapsedLabel = "source_elapsed";
    private final String threadLabel = "thread";
    private final String pipe = " | ";

    private int activityLength = activityLabel.length();
    private int sourceLength = sourceLabel.length();
    private int sourceElapsedLength = sourceElapsedLabel.length();
    private int threadLength = threadLabel.length();

    private final List<String> traceEvents = new LinkedList<>();

    public TraceEventFormatter(QueryTrace queryTrace) {
        this.queryTrace = queryTrace;
        traceEvents.add(String.format("%s to %s took %dμs", queryTrace.getRequestType(), queryTrace.getCoordinatorAddress(), queryTrace.getDurationMicros()));
        traceEvents.add(String.format("Tracing ID: %s%n", queryTrace.getTracingId()));

        calculateColumnWidths();
        buildHeading();
        buildHeadingBorder();

        for (TraceEvent event : queryTrace.getEvents()) {
            buildEvent(event);
        }
    }

    // Finds the width of each column for padding
    private void calculateColumnWidths() {
        for (TraceEvent event : queryTrace.getEvents()) {
            activityLength = Math.max(event.getActivity().length(), activityLength);
            sourceLength = Math.max(event.getSourceAddress().toString().length(), sourceLength);
            sourceElapsedLength = Math.max(String.valueOf(event.getSourceElapsedMicros()).length(), sourceElapsedLength);
            threadLength = Math.max(event.getThreadName().length(), threadLength);
        }
    }

    // Pad strings: +length = left padding, -length = right padding
    private String padString(String string, int length) {
        return String.format("%" + length + "s", string);
    }

    // Build dashes for use below headings
    private String buildDashes(int length) {
        StringBuilder dashString = new StringBuilder();
        for (int i = 0; i < length; i++) {
            dashString.append("-");
        }
        return dashString.toString();
    }

    // Build heading
    private void buildHeading() {
        List<String> elements = new LinkedList<>();
        elements.add(padString(activityLabel, -activityLength));
        elements.add(padString(sourceLabel, -sourceLength));
        elements.add(padString(sourceElapsedLabel, -sourceElapsedLength));
        elements.add(padString(threadLabel, -threadLength));

        String header = formatRow(elements, pipe);

        traceEvents.add(header);
    }

    // Build heading underline
    private void buildHeadingBorder() {
        String plus = "-+-";

        List<String> elements = new LinkedList<>();
        elements.add(buildDashes(activityLength));
        elements.add(buildDashes(sourceLength));
        elements.add(buildDashes(sourceElapsedLength));
        elements.add(buildDashes(threadLength));

        String border = formatRow(elements, plus);

        traceEvents.add(border);
    }

    // Build event
    private void buildEvent(TraceEvent event) {
        List<String> elements = new LinkedList<>();
        elements.add(padString(event.getActivity(), activityLength));
        elements.add(padString(event.getSourceAddress().toString(), sourceLength));
        elements.add(padString(String.valueOf(event.getSourceElapsedMicros()), sourceElapsedLength));
        elements.add(padString(event.getThreadName(), threadLength));

        String eventString = formatRow(elements, pipe);

        traceEvents.add(eventString);
    }

    private String formatRow(List<String> elements, String deliminator) {
        StringBuilder row = new StringBuilder();

        for (int i = 0; i < elements.size(); i++) {
            String item = elements.get(i);

            row.append(item);

            // No pipe after last element
            if (i != elements.size() -1) {
                row.append(deliminator);
            }
        }

        return row.toString();
    }

    @Override
    public String toString() {
        StringBuilder table = new StringBuilder();
        for (String row : traceEvents) {
            table.append(row).append("\n");
        }
        return table.toString();
    }
}
