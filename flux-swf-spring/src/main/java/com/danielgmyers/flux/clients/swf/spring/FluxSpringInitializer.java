package com.danielgmyers.flux.clients.swf.spring;

import java.util.List;

import com.danielgmyers.flux.FluxCapacitor;
import com.danielgmyers.flux.wf.Workflow;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Initializes a previously created FluxCapacitor object.
 */
public final class FluxSpringInitializer {

    @Autowired
    public FluxSpringInitializer(FluxCapacitor fc, List<Workflow> workflows) {
        fc.initialize(workflows);
    }

}
