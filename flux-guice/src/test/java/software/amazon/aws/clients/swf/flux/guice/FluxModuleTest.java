package software.amazon.aws.clients.swf.flux.guice;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import software.amazon.aws.clients.swf.flux.wf.Workflow;
import software.amazon.aws.clients.swf.flux.guice.workflowone.WorkflowOne;
import software.amazon.aws.clients.swf.flux.guice.workflowtwo.WorkflowTwo;

public class FluxModuleTest {

    @Test
    public void testWorkflowClassScanningFindsAllWorkflows() {
        FluxModule module = new FluxModule("software.amazon.aws.clients.swf.flux.guice");
        // we don't need to verify that everything's there, just that *something* is there
        List<Class<? extends Workflow>> classes = module.findWorkflowClassesFromClasspath();
        Assertions.assertEquals(2, classes.size());
        Assertions.assertTrue(classes.contains(WorkflowOne.class));
        Assertions.assertTrue(classes.contains(WorkflowTwo.class));
    }

    @Test
    public void testWorkflowClassScanningScopingExcludesWorkflowsOutsideScope() {
        FluxModule module = new FluxModule("software.amazon.aws.clients.swf.flux.guice.workflowone");
        // we don't need to verify that everything's there, just that *something* is there
        List<Class<? extends Workflow>> classes = module.findWorkflowClassesFromClasspath();
        Assertions.assertEquals(1, classes.size());
        Assertions.assertTrue(classes.contains(WorkflowOne.class));
        Assertions.assertFalse(classes.contains(WorkflowTwo.class));
    }
}
