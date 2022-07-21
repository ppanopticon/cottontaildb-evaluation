package org.vitrivr.cottontail.evaluation.constants

import jetbrains.letsPlot.elementText
import jetbrains.letsPlot.scale.scaleColorManual
import jetbrains.letsPlot.scale.scaleFillManual
import jetbrains.letsPlot.theme

/**  The theme used by all plots. */
val THEME = theme(
    axisTitle = elementText("#2D373C", face = "bold"),
    axisText = elementText("#2D373C"),
    stripText = elementText("#2D373C", face = "bold"),
)

/**  The scale (line) colors used by all plots. */
val SCALE_COLORS = scaleColorManual(listOf("#A5D7D2", "#D20F37"))

/**  The fill colors used by all plots. */
val SCALE_FILLS = scaleFillManual(listOf("#D2EBE9", "#DD879B"))
