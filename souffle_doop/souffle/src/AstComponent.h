/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file AstComponent.h
 *
 * Defines the class utilized to model a component within the input program.
 *
 ***********************************************************************/

#pragma once

#include "AstRelation.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace souffle {

class ErrorReport;

/**
 * A component type is the class utilized to represent a construct of the form
 *
 *                  name < Type, Type, ... >
 *
 * where name is the name of the component and < Type, Type, ... > is an optional
 * list of type parameters.
 */
class AstComponentType : public AstNode {
    /**
     * The name of the addressed component.
     */
    std::string name;

    /**
     * The list of associated type parameters.
     */
    std::vector<AstTypeIdentifier> typeParams;

public:
    /**
     * Creates a new component type based on the given name and parameters.
     */
    AstComponentType(
            std::string name = "", std::vector<AstTypeIdentifier> params = std::vector<AstTypeIdentifier>())
            : name(std::move(name)), typeParams(std::move(params)) {}

    // -- getters and setters --

    const std::string& getName() const {
        return name;
    }

    void setName(const std::string& n) {
        name = n;
    }

    const std::vector<AstTypeIdentifier>& getTypeParameters() const {
        return typeParams;
    }

    void setTypeParameters(const std::vector<AstTypeIdentifier>& params) {
        typeParams = params;
    }

    // -- others --

    std::vector<const AstNode*> getChildNodes() const override {
        std::vector<const AstNode*> res;
        return res;  // no child nodes
    }

    void apply(const AstNodeMapper& /*mapper*/) override {
        // nothing to do
    }

    AstComponentType* clone() const override {
        AstComponentType* res = new AstComponentType(name, typeParams);
        res->setSrcLoc(getSrcLoc());
        return res;
    }

    void print(std::ostream& os) const override {
        os << name;
        if (!typeParams.empty()) {
            os << "<" << join(typeParams, ",") << ">";
        }
    }

protected:
    bool equal(const AstNode& node) const override {
        assert(nullptr != dynamic_cast<const AstComponentType*>(&node));
        const auto& other = static_cast<const AstComponentType&>(node);
        return name == other.name && typeParams == other.typeParams;
    }
};

/**
 * A node type representing expressions utilized to initialize components by
 * binding them to a name.
 */
class AstComponentInit : public AstNode {
    /**
     * The name of the resulting component instance.
     */
    std::string instanceName;

    /**
     * The type of the component to be instantiated.
     */
    std::unique_ptr<AstComponentType> componentType;

public:
    // -- getters and setters --

    const std::string& getInstanceName() const {
        return instanceName;
    }

    void setInstanceName(const std::string& name) {
        instanceName = name;
    }

    const AstComponentType* getComponentType() const {
        return componentType.get();
    }

    void setComponentType(std::unique_ptr<AstComponentType> type) {
        componentType = std::move(type);
    }

    /** Requests an independent, deep copy of this node */
    AstComponentInit* clone() const override {
        auto res = new AstComponentInit();
        res->setComponentType(std::unique_ptr<AstComponentType>(componentType->clone()));
        res->setInstanceName(instanceName);
        return res;
    }

    /** Applies the node mapper to all child nodes and conducts the corresponding replacements */
    void apply(const AstNodeMapper& mapper) override {
        componentType = mapper(std::move(componentType));
    }

    /** Obtains a list of all embedded child nodes */
    std::vector<const AstNode*> getChildNodes() const override {
        std::vector<const AstNode*> res;
        res.push_back(componentType.get());
        return res;
    }

    /** Output to a given output stream */
    void print(std::ostream& os) const override {
        os << ".init " << instanceName << " = ";
        componentType->print(os);
    }

protected:
    /** An internal function to determine equality to another node */
    bool equal(const AstNode& node) const override {
        assert(nullptr != dynamic_cast<const AstComponentInit*>(&node));
        const auto& other = static_cast<const AstComponentInit&>(node);
        return instanceName == other.instanceName && componentType == other.componentType;
    }
};

/**
 * A AST node describing a component within the input program.
 */
class AstComponent : public AstNode {
    /**
     * The type of this component, including its name and type parameters.
     */
    std::unique_ptr<AstComponentType> type;

    /**
     * A list of base types to inherit relations and clauses from.
     */
    std::vector<std::unique_ptr<AstComponentType>> baseComponents;

    /**
     * A list of types declared in this component.
     */
    std::vector<std::unique_ptr<AstType>> types;

    /**
     * A list of relations declared in this component.
     */
    std::vector<std::unique_ptr<AstRelation>> relations;

    /**
     * A list of clauses defined in this component.
     */
    std::vector<std::unique_ptr<AstClause>> clauses;

    /**
     * A list of IO directives defined in this component.
     */
    std::vector<std::unique_ptr<AstIODirective>> ioDirectives;

    /**
     * A list of nested components.
     */
    std::vector<std::unique_ptr<AstComponent>> components;

    /**
     * A list of nested component instantiations.
     */
    std::vector<std::unique_ptr<AstComponentInit>> instantiations;

    /**
     * Set of relations that are overwritten
     */
    std::set<std::string> overrideRules;

public:
    ~AstComponent() override = default;

    // -- getters and setters --

    const AstComponentType* getComponentType() const {
        return type.get();
    }

    void setComponentType(std::unique_ptr<AstComponentType> other) {
        type = std::move(other);
    }

    const std::vector<AstComponentType*> getBaseComponents() const {
        return toPtrVector(baseComponents);
    }

    void addBaseComponent(std::unique_ptr<AstComponentType> component) {
        baseComponents.push_back(std::move(component));
    }

    void addType(std::unique_ptr<AstType> t) {
        types.push_back(std::move(t));
    }

    std::vector<AstType*> getTypes() const {
        return toPtrVector(types);
    }

    void copyBaseComponents(const AstComponent* other) {
        baseComponents.clear();
        for (const auto& baseComponent : other->getBaseComponents()) {
            baseComponents.push_back(std::unique_ptr<AstComponentType>(baseComponent->clone()));
        }
    }

    void addRelation(std::unique_ptr<AstRelation> r) {
        relations.push_back(std::move(r));
    }

    std::vector<AstRelation*> getRelations() const {
        return toPtrVector(relations);
    }

    void addClause(std::unique_ptr<AstClause> c) {
        clauses.push_back(std::move(c));
    }

    std::vector<AstClause*> getClauses() const {
        return toPtrVector(clauses);
    }

    void addIODirective(std::unique_ptr<AstIODirective> c) {
        ioDirectives.push_back(std::move(c));
    }

    void addIODirectiveChain(std::unique_ptr<AstIODirective> c) {
        for (auto& currentName : c->getNames()) {
            std::unique_ptr<AstIODirective> clone(c->clone());
            clone->setName(currentName);
            ioDirectives.push_back(std::move(clone));
        }
    }

    std::vector<AstIODirective*> getIODirectives() const {
        return toPtrVector(ioDirectives);
    }

    void addComponent(std::unique_ptr<AstComponent> c) {
        components.push_back(std::move(c));
    }

    std::vector<AstComponent*> getComponents() const {
        return toPtrVector(components);
    }

    void addInstantiation(std::unique_ptr<AstComponentInit> i) {
        instantiations.push_back(std::move(i));
    }

    std::vector<AstComponentInit*> getInstantiations() const {
        return toPtrVector(instantiations);
    }

    void addOverride(const std::string& name) {
        overrideRules.insert(name);
    }

    const std::set<std::string>& getOverridden() const {
        return overrideRules;
    }

    /** Requests an independent, deep copy of this node */
    AstComponent* clone() const override {
        auto* res = new AstComponent();
        res->setComponentType(std::unique_ptr<AstComponentType>(type->clone()));

        for (const auto& cur : baseComponents) {
            res->baseComponents.push_back(std::unique_ptr<AstComponentType>(cur->clone()));
        }
        for (const auto& cur : components) {
            res->components.push_back(std::unique_ptr<AstComponent>(cur->clone()));
        }
        for (const auto& cur : instantiations) {
            res->instantiations.push_back(std::unique_ptr<AstComponentInit>(cur->clone()));
        }
        for (const auto& cur : types) {
            res->types.push_back(std::unique_ptr<AstType>(cur->clone()));
        }
        for (const auto& cur : relations) {
            res->relations.push_back(std::unique_ptr<AstRelation>(cur->clone()));
        }
        for (const auto& cur : clauses) {
            res->clauses.push_back(std::unique_ptr<AstClause>(cur->clone()));
        }
        for (const auto& cur : ioDirectives) {
            res->ioDirectives.push_back(std::unique_ptr<AstIODirective>(cur->clone()));
        }
        for (const auto& cur : overrideRules) {
            res->overrideRules.insert(cur);
        }

        return res;
    }

    /** Applies the node mapper to all child nodes and conducts the corresponding replacements */
    void apply(const AstNodeMapper& mapper) override {
        // apply mapper to all sub-nodes
        type = mapper(std::move(type));
        for (auto& cur : components) {
            cur = mapper(std::move(cur));
        }
        for (auto& cur : baseComponents) {
            cur = mapper(std::move(cur));
        }
        for (auto& cur : instantiations) {
            cur = mapper(std::move(cur));
        }
        for (auto& cur : types) {
            cur = mapper(std::move(cur));
        }
        for (auto& cur : relations) {
            cur = mapper(std::move(cur));
        }
        for (auto& cur : clauses) {
            cur = mapper(std::move(cur));
        }
        for (auto& cur : ioDirectives) {
            cur = mapper(std::move(cur));
        }
    }

    /** Obtains a list of all embedded child nodes */
    std::vector<const AstNode*> getChildNodes() const override {
        std::vector<const AstNode*> res;

        res.push_back(type.get());
        for (const auto& cur : components) {
            res.push_back(cur.get());
        }
        for (const auto& cur : baseComponents) {
            res.push_back(cur.get());
        }
        for (const auto& cur : instantiations) {
            res.push_back(cur.get());
        }
        for (const auto& cur : types) {
            res.push_back(cur.get());
        }
        for (const auto& cur : relations) {
            res.push_back(cur.get());
        }
        for (const auto& cur : clauses) {
            res.push_back(cur.get());
        }
        for (const auto& cur : ioDirectives) {
            res.push_back(cur.get());
        }

        return res;
    }

    /** Output to a given output stream */
    void print(std::ostream& os) const override {
        os << ".comp " << getComponentType() << " ";

        if (!baseComponents.empty()) {
            os << ": " << join(baseComponents, ",", print_deref<std::unique_ptr<AstComponentType>>()) << " ";
        }
        os << "{\n";

        if (!components.empty()) {
            os << join(components, "\n", print_deref<std::unique_ptr<AstComponent>>()) << "\n";
        }
        if (!instantiations.empty()) {
            os << join(instantiations, "\n", print_deref<std::unique_ptr<AstComponentInit>>()) << "\n";
        }
        if (!types.empty()) {
            os << join(types, "\n", print_deref<std::unique_ptr<AstType>>()) << "\n";
        }
        if (!relations.empty()) {
            os << join(relations, "\n", print_deref<std::unique_ptr<AstRelation>>()) << "\n";
        }
        for (const auto& cur : overrideRules) {
            os << ".override " << cur << "\n";
        }
        if (!clauses.empty()) {
            os << join(clauses, "\n\n", print_deref<std::unique_ptr<AstClause>>()) << "\n";
        }
        if (!ioDirectives.empty()) {
            os << join(ioDirectives, "\n\n", print_deref<std::unique_ptr<AstIODirective>>()) << "\n";
        }

        os << "}\n";
    }

protected:
    /** An internal function to determine equality to another node */
    bool equal(const AstNode& node) const override {
        assert(nullptr != dynamic_cast<const AstComponent*>(&node));
        const auto& other = static_cast<const AstComponent&>(node);

        // compare all fields
        return type == other.type && baseComponents == other.baseComponents &&
               equal_targets(types, other.types) && equal_targets(relations, other.relations) &&
               equal_targets(clauses, other.clauses) && equal_targets(ioDirectives, other.ioDirectives) &&
               equal_targets(components, other.components) &&
               equal_targets(instantiations, other.instantiations);
    }
};

}  // end of namespace souffle
