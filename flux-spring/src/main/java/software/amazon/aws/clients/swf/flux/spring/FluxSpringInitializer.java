package software.amazon.aws.clients.swf.flux.spring;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import software.amazon.aws.clients.swf.flux.FluxCapacitor;
import software.amazon.aws.clients.swf.flux.wf.Workflow;

/**
 * Initializes a previously created FluxCapacitor object.
 */
public final class FluxSpringInitializer {

    @Autowired
    public FluxSpringInitializer(FluxCapacitor fc, List<Workflow> workflows) {
        fc.initialize(workflows);
    }

}
