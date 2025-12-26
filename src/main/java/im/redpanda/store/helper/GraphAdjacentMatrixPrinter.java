package im.redpanda.store.helper;

import im.redpanda.core.Node;
import im.redpanda.store.NodeEdge;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.DecimalFormat;
import org.jetbrains.annotations.NotNull;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.nio.csv.CSVExporter;
import org.jgrapht.nio.csv.CSVFormat;

public class GraphAdjacentMatrixPrinter {

  public static final String ANSI_RESET = "\u001B[0m";
  public static final String ANSI_BLACK = "\u001B[30m";
  public static final String ANSI_RED = "\u001B[31m";
  public static final String ANSI_GREEN = "\u001B[32m";
  public static final String ANSI_YELLOW = "\u001B[33m";
  public static final String ANSI_BLUE = "\u001B[34m";
  public static final String ANSI_PURPLE = "\u001B[35m";
  public static final String ANSI_CYAN = "\u001B[36m";
  public static final String ANSI_WHITE = "\u001B[37m";
  public static final String defaultBadValue = ANSI_RED + "15" + ANSI_RESET;

  public static void printGraph(DefaultDirectedWeightedGraph<Node, NodeEdge> nodeGraph) {
    CSVExporter<Node, NodeEdge> exporter = new CSVExporter<>(CSVFormat.MATRIX);
    exporter.setParameter(CSVFormat.Parameter.EDGE_WEIGHTS, true);
    exporter.setParameter(CSVFormat.Parameter.MATRIX_FORMAT_ZERO_WHEN_NO_EDGE, true);
    exporter.setVertexIdProvider(node -> node.toString());

    Writer writer = new StringWriter();
    exporter.exportGraph(nodeGraph, writer);

    Writer formattedWriter = postFormatValues(writer);

    System.out.println(formattedWriter);
  }

  @NotNull
  private static Writer postFormatValues(Writer writer) {

    Writer w = new StringWriter();
    DecimalFormat decimalFormat = new DecimalFormat("#");
    try {

      for (String line : writer.toString().split("\n")) {

        String[] values = line.split(",");
        int entryCount = 0;
        for (String value : values) {

          if (value.isEmpty()) {
            w.write(defaultBadValue);
          } else {
            double v = Double.parseDouble(value);
            if (v < 1) {
              w.write(defaultBadValue);
            } else if (v > 13) {
              w.write(ANSI_RED + decimalFormat.format(v) + ANSI_RESET);
            } else if (v < 3) {
              w.write(ANSI_GREEN + decimalFormat.format(v) + ANSI_RESET);
            } else {
              w.write(decimalFormat.format(v));
            }
          }

          entryCount++;
          if (entryCount != values.length) {
            w.write(",");
          }
        }
        w.write("\n");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return w;
  }
}
