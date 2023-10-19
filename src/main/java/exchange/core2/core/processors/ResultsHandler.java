package exchange.core2.core.processors;

import com.lmax.disruptor.EventHandler;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import lombok.RequiredArgsConstructor;

import java.util.function.ObjLongConsumer;

@RequiredArgsConstructor
public final class ResultsHandler implements EventHandler<OrderCommand> {

    private final ObjLongConsumer<OrderCommand> resultsConsumer;

    private boolean processingEnabled = true;

    @Override
    public void onEvent(OrderCommand cmd, long sequence, boolean endOfBatch) {

        if (cmd.command == OrderCommandType.GROUPING_CONTROL) {
            processingEnabled = cmd.orderId == 1; //重放全部时， orderId = mode = 0 , enableJournaling 时， orderId = mode = 1
        }

        if (processingEnabled) {
            //重放全部时， 不进入执行， enableJournaling 时进入执行
            resultsConsumer.accept(cmd, sequence);
        }

    }
}
