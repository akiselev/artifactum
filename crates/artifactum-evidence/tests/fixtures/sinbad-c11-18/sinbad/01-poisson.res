module corpus.scalar.poisson;

model Poisson {
    domain Omega { dimension = 2; coordinates = cartesian; }

    field u: unknown scalar H1(order=1) on Omega;
    provider diffusivity(material: selector) -> Diffusivity { differentiability = analytic_provided; }
    provider exact_u( ) -> Dimensionless { differentiability = analytic_provided; }

    property k = diffusivity(0);
    source f: VolumetricSource;

    equation balance on Omega {
        -div(k * grad(u)) = f;
    }

    boundary walls on boundary("walls") {
        dirichlet u = exact_u();
    }

    observable energy { integrate(0.5 * k * dot(grad(u), grad(u))); }

    @mms(field = u);
    @validation(dataset = "internal-mms");
}
