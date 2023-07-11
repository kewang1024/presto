/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.AggregationFunctionImplementation;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StatisticAggregations
{
    // outputVariables indicates the order of aggregations in the output
    private final List<VariableReferenceExpression> outputVariables;
    private final Map<VariableReferenceExpression, Aggregation> aggregations;
    private final List<VariableReferenceExpression> groupingVariables;

    @JsonCreator
    public StatisticAggregations(
            @JsonProperty("outputVariables") List<VariableReferenceExpression> outputVariables,
            @JsonProperty("aggregations") Map<VariableReferenceExpression, Aggregation> aggregations,
            @JsonProperty("groupingVariables") List<VariableReferenceExpression> groupingVariables)
    {
        this.outputVariables = ImmutableList.copyOf(requireNonNull(outputVariables, "outputVariables is null"));
        this.aggregations = ImmutableMap.copyOf(requireNonNull(aggregations, "aggregations is null"));
        this.groupingVariables = ImmutableList.copyOf(requireNonNull(groupingVariables, "groupingVariables is null"));
        checkArgument(outputVariables.size() == aggregations.size(), "outputVariables and aggregations' sizes are different");
    }

    public StatisticAggregations(
            Map<VariableReferenceExpression, Aggregation> aggregations,
            List<VariableReferenceExpression> groupingVariables)
    {
        this.aggregations = ImmutableMap.copyOf(requireNonNull(aggregations, "aggregations is null"));
        this.groupingVariables = ImmutableList.copyOf(requireNonNull(groupingVariables, "groupingVariables is null"));
        this.outputVariables = ImmutableList.copyOf(aggregations.keySet());
    }

    @JsonProperty
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return outputVariables;
    }

    @JsonProperty
    public Map<VariableReferenceExpression, Aggregation> getAggregations()
    {
        return aggregations;
    }

    @JsonProperty
    public List<VariableReferenceExpression> getGroupingVariables()
    {
        return groupingVariables;
    }

    public Parts splitIntoPartialAndFinal(Session session, VariableAllocator variableAllocator, FunctionAndTypeManager functionAndTypeManager)
    {
        return split(session, variableAllocator, functionAndTypeManager, false);
    }

    public Parts splitIntoPartialAndIntermediate(Session session, VariableAllocator variableAllocator, FunctionAndTypeManager functionAndTypeManager)
    {
        return split(session, variableAllocator, functionAndTypeManager, true);
    }

    private Parts split(Session session, VariableAllocator variableAllocator, FunctionAndTypeManager functionAndTypeManager, boolean intermediate)
    {
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> finalOrIntermediateAggregations = ImmutableMap.builder();
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> partialAggregations = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : aggregations.entrySet()) {
            Aggregation originalAggregation = entry.getValue();
            FunctionHandle functionHandle = originalAggregation.getFunctionHandle();
            AggregationFunctionImplementation function = functionAndTypeManager.getAggregateFunctionImplementation(functionHandle);
            FunctionHandle partialAggregation = null;
            FunctionHandle finalAggregation = null;
            ImmutableList.Builder<TypeSignature> types = ImmutableList.builder();
            for (int i = 0; i < originalAggregation.getArguments().size(); i++) {
                types.add(originalAggregation.getArguments().get(i).getType().getTypeSignature());
            }
            if (originalAggregation.getCall().getDisplayName().equals("approx_distinct")) {
                partialAggregation = functionAndTypeManager.resolveFunction(
                        Optional.of(session.getSessionFunctions()),
                        session.getTransactionId(),
                        QualifiedObjectName.valueOf("presto.default.approx_set"),
                        fromTypeSignatures(types.build()));
                finalAggregation = functionAndTypeManager.resolveFunction(
                        Optional.of(session.getSessionFunctions()),
                        session.getTransactionId(),
                        QualifiedObjectName.valueOf("presto.default.merge"),
                        fromTypeSignatures(new TypeSignature("HyperLogLog")));
            }

            // create partial aggregation
            VariableReferenceExpression partialVariable = variableAllocator.newVariable(entry.getValue().getCall().getSourceLocation(), functionAndTypeManager.getFunctionMetadata(functionHandle).getName().getObjectName(), function.getIntermediateType());
            partialAggregations.put(partialVariable, new Aggregation(
                    new CallExpression(
                            originalAggregation.getCall().getSourceLocation(),
                            partialAggregation == null ? originalAggregation.getCall().getDisplayName() : partialAggregation.getName(),
                            partialAggregation == null ? functionHandle : partialAggregation,
                            function.getIntermediateType(),
                            originalAggregation.getArguments()),
                    originalAggregation.getFilter(),
                    originalAggregation.getOrderBy(),
                    originalAggregation.isDistinct(),
                    originalAggregation.getMask()));

            // create final aggregation
            finalOrIntermediateAggregations.put(entry.getKey(),
                    new Aggregation(
                            new CallExpression(
                                    originalAggregation.getCall().getSourceLocation(),
                                    finalAggregation == null ? originalAggregation.getCall().getDisplayName() : finalAggregation.getName(),
                                    finalAggregation == null ? functionHandle : finalAggregation,
                                    function.getFinalType(),
                                    ImmutableList.of(partialVariable)),
                            Optional.empty(),
                            Optional.empty(),
                            false,
                            Optional.empty()));
        }

        StatisticAggregations finalOrIntermediateAggregation = new StatisticAggregations(finalOrIntermediateAggregations.build(), groupingVariables);
        return new Parts(
                intermediate ? Optional.empty() : Optional.of(finalOrIntermediateAggregation),
                intermediate ? Optional.of(finalOrIntermediateAggregation) : Optional.empty(),
                new StatisticAggregations(partialAggregations.build(), groupingVariables));
    }

    public static class Parts
    {
        private final Optional<StatisticAggregations> finalAggregation;
        private final Optional<StatisticAggregations> intermediateAggregation;
        private final StatisticAggregations partialAggregation;

        public Parts(
                Optional<StatisticAggregations> finalAggregation,
                Optional<StatisticAggregations> intermediateAggregation,
                StatisticAggregations partialAggregation)
        {
            this.finalAggregation = requireNonNull(finalAggregation, "finalAggregation is null");
            this.intermediateAggregation = requireNonNull(intermediateAggregation, "intermediateAggregation is null");
            checkArgument(
                    finalAggregation.isPresent() ^ intermediateAggregation.isPresent(),
                    "only final or only intermediate aggregation is expected to be present");
            this.partialAggregation = requireNonNull(partialAggregation, "partialAggregation is null");
        }

        public StatisticAggregations getFinalAggregation()
        {
            return finalAggregation.orElseThrow(() -> new IllegalStateException("finalAggregation is not present"));
        }

        public StatisticAggregations getIntermediateAggregation()
        {
            return intermediateAggregation.orElseThrow(() -> new IllegalStateException("intermediateAggregation is not present"));
        }

        public StatisticAggregations getPartialAggregation()
        {
            return partialAggregation;
        }
    }
}
